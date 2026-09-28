use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytesize::ByteSize;
use tonic::{Request, Response, Status};
use tracing::{info, warn};

use crate::mounts;
use crate::store::Store;

pub mod proto {
    #![allow(clippy::all, clippy::pedantic)]
    tonic::include_proto!("csi.v1");
}

use proto::identity_server::Identity;
use proto::node_server::Node;
use proto::{
    GetPluginCapabilitiesRequest, GetPluginCapabilitiesResponse, GetPluginInfoRequest,
    GetPluginInfoResponse, NodeExpandVolumeRequest, NodeExpandVolumeResponse,
    NodeGetCapabilitiesRequest, NodeGetCapabilitiesResponse, NodeGetInfoRequest,
    NodeGetInfoResponse, NodeGetStorageHealthRequest, NodeGetStorageHealthResponse,
    NodeGetVolumeHealthRequest, NodeGetVolumeHealthResponse, NodeGetVolumeStatsRequest,
    NodeGetVolumeStatsResponse, NodePublishVolumeRequest, NodePublishVolumeResponse,
    NodeStageVolumeRequest, NodeStageVolumeResponse, NodeUnpublishVolumeRequest,
    NodeUnpublishVolumeResponse, NodeUnstageVolumeRequest, NodeUnstageVolumeResponse, ProbeRequest,
    ProbeResponse,
};

/// Space-separated store paths, or paths inside them.
pub const CLOSURES: &str = "closures";

/// Kubelet gives up on a call after about two minutes, so a publish tells it
/// to retry before then.
const PUBLISH_WAIT: Duration = Duration::from_secs(90);

pub struct Plugin {
    name: String,
    node_id: String,
    store: Arc<Store>,
    /// Calls for a volume take turns, including a publish that kubelet gave
    /// up on, which keeps running.
    volumes: Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
}

impl Plugin {
    pub fn new(name: String, node_id: String, store: Arc<Store>) -> Self {
        Self {
            name,
            node_id,
            store,
            volumes: Mutex::default(),
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    /// Runs `f` on a blocking thread in the volume's turn, with collection held
    /// off. Both stay so until `f` ends, even if kubelet gives up.
    async fn for_volume(
        &self,
        volume: &str,
        f: impl FnOnce(&Store) -> anyhow::Result<()> + Send + 'static,
    ) -> Result<(), Status> {
        let lock = {
            let mut volumes = self.volumes.lock().unwrap();
            // Otherwise the map keeps every volume there ever was.
            volumes.retain(|_, lock| Arc::strong_count(lock) > 1);
            volumes.entry(volume.to_owned()).or_default().clone()
        };
        let held = (lock.lock_owned().await, self.store.gc_guard().await);
        let store = self.store.clone();
        tokio::task::spawn_blocking(move || {
            let _held = held;
            f(&store)
        })
        .await
        .map_err(|e| Status::internal(e.to_string()))?
        .map_err(|e| Status::internal(format!("{e:#}")))
    }
}

#[tonic::async_trait]
impl Identity for Plugin {
    async fn get_plugin_info(
        &self,
        _: Request<GetPluginInfoRequest>,
    ) -> Result<Response<GetPluginInfoResponse>, Status> {
        Ok(Response::new(GetPluginInfoResponse {
            name: self.name.clone(),
            vendor_version: env!("CARGO_PKG_VERSION").into(),
            ..Default::default()
        }))
    }

    async fn get_plugin_capabilities(
        &self,
        _: Request<GetPluginCapabilitiesRequest>,
    ) -> Result<Response<GetPluginCapabilitiesResponse>, Status> {
        // Volumes are inline, so there is no controller service.
        Ok(Response::new(GetPluginCapabilitiesResponse::default()))
    }

    async fn probe(&self, _: Request<ProbeRequest>) -> Result<Response<ProbeResponse>, Status> {
        Ok(Response::new(ProbeResponse { ready: Some(true) }))
    }
}

#[tonic::async_trait]
impl Node for Plugin {
    async fn node_publish_volume(
        &self,
        request: Request<NodePublishVolumeRequest>,
    ) -> Result<Response<NodePublishVolumeResponse>, Status> {
        let request = request.into_inner();
        if request.target_path.is_empty() {
            return Err(Status::invalid_argument("no target path"));
        }
        // The container runtime binds the target into the container again,
        // writable unless the volume is read-only.
        if !request.readonly {
            return Err(Status::invalid_argument("the volume needs readOnly: true"));
        }
        let closures = (request.volume_context.get(CLOSURES))
            .ok_or_else(|| Status::invalid_argument(format!("no {CLOSURES} attribute")))?;
        let roots = (self.store.roots(closures.split_whitespace()))
            .map_err(|e| Status::invalid_argument(format!("{e:#}")))?;
        if roots.is_empty() {
            return Err(Status::invalid_argument(format!("{CLOSURES} is empty")));
        }

        let mut ensuring = self.store.ensure(&roots);
        let names = match tokio::time::timeout(PUBLISH_WAIT, ensuring.done()).await {
            Ok(Ok(names)) => names,
            Ok(Err(e)) => {
                warn!(volume = request.volume_id, "{e:#}");
                return Err(Status::internal(format!("{e:#}")));
            }
            Err(_) => {
                // Kubelet puts it in the pod's events.
                let msg = match ensuring.progress() {
                    Some((done, total)) => format!(
                        "still fetching the closure, {} of {}",
                        ByteSize(done),
                        ByteSize(total)
                    ),
                    None => "still resolving the closure".to_owned(),
                };
                info!(volume = request.volume_id, "{msg}");
                return Err(Status::unavailable(msg));
            }
        };

        let target = std::path::PathBuf::from(&request.target_path);
        let volume = request.volume_id.clone();
        self.for_volume(&request.volume_id, move |store| {
            let view = store.view(&volume, &names, &target)?;
            mounts::publish(&view, &target)
        })
        .await?;
        info!(volume = request.volume_id, ?roots, "published");
        Ok(Response::new(NodePublishVolumeResponse {}))
    }

    async fn node_unpublish_volume(
        &self,
        request: Request<NodeUnpublishVolumeRequest>,
    ) -> Result<Response<NodeUnpublishVolumeResponse>, Status> {
        let request = request.into_inner();
        let target = std::path::PathBuf::from(&request.target_path);
        let volume = request.volume_id.clone();
        self.for_volume(&request.volume_id, move |store| {
            mounts::unpublish(&target)?;
            store.drop_view(&volume)
        })
        .await?;
        info!(volume = request.volume_id, "unpublished");
        Ok(Response::new(NodeUnpublishVolumeResponse {}))
    }

    async fn node_get_capabilities(
        &self,
        _: Request<NodeGetCapabilitiesRequest>,
    ) -> Result<Response<NodeGetCapabilitiesResponse>, Status> {
        Ok(Response::new(NodeGetCapabilitiesResponse::default()))
    }

    async fn node_get_info(
        &self,
        _: Request<NodeGetInfoRequest>,
    ) -> Result<Response<NodeGetInfoResponse>, Status> {
        Ok(Response::new(NodeGetInfoResponse {
            node_id: self.node_id.clone(),
            ..Default::default()
        }))
    }

    async fn node_stage_volume(
        &self,
        _: Request<NodeStageVolumeRequest>,
    ) -> Result<Response<NodeStageVolumeResponse>, Status> {
        Err(Status::unimplemented("no staging"))
    }

    async fn node_unstage_volume(
        &self,
        _: Request<NodeUnstageVolumeRequest>,
    ) -> Result<Response<NodeUnstageVolumeResponse>, Status> {
        Err(Status::unimplemented("no staging"))
    }

    async fn node_get_volume_stats(
        &self,
        _: Request<NodeGetVolumeStatsRequest>,
    ) -> Result<Response<NodeGetVolumeStatsResponse>, Status> {
        Err(Status::unimplemented("no volume stats"))
    }

    async fn node_expand_volume(
        &self,
        _: Request<NodeExpandVolumeRequest>,
    ) -> Result<Response<NodeExpandVolumeResponse>, Status> {
        Err(Status::unimplemented("no expansion"))
    }

    async fn node_get_volume_health(
        &self,
        _: Request<NodeGetVolumeHealthRequest>,
    ) -> Result<Response<NodeGetVolumeHealthResponse>, Status> {
        Err(Status::unimplemented("no volume health"))
    }

    async fn node_get_storage_health(
        &self,
        _: Request<NodeGetStorageHealthRequest>,
    ) -> Result<Response<NodeGetStorageHealthResponse>, Status> {
        Err(Status::unimplemented("no storage health"))
    }
}
