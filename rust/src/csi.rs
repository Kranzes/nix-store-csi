use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio_util::sync::CancellationToken;
use tonic::{Request, Response, Status};
use tracing::{info, instrument, warn};

use crate::mounts;
use crate::store::{self, Store};
use crate::store_path::{self, Names, STORE_PATHS, system};

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
    NodeServiceCapability, NodeStageVolumeRequest, NodeStageVolumeResponse,
    NodeUnpublishVolumeRequest, NodeUnpublishVolumeResponse, NodeUnstageVolumeRequest,
    NodeUnstageVolumeResponse, ProbeRequest, ProbeResponse, VolumeUsage, node_service_capability,
    volume_usage,
};

/// The name pods give as the volume's `driver`.
pub const DRIVER: &str = "nix-store-csi";

/// Kubelet gives up on a call after about two minutes, so a publish tells it
/// to retry before then.
const PUBLISH_WAIT: Duration = Duration::from_secs(90);

pub struct Plugin {
    node_id: String,
    store: Arc<Store>,
    /// Calls for a volume take turns. A publish that kubelet gave up on keeps
    /// running and keeps its turn.
    volumes: Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
    /// When kubelet first asked to publish each volume that still waits for
    /// its closure.
    waiting: Mutex<HashMap<String, Instant>>,
    shutdown: CancellationToken,
}

impl Plugin {
    pub fn new(node_id: String, store: Arc<Store>) -> Self {
        Self {
            node_id,
            store,
            volumes: Mutex::default(),
            waiting: Mutex::default(),
            shutdown: CancellationToken::new(),
        }
    }

    /// Ends the publishes still waiting for their closures, since the server
    /// waits for every call to end before it shuts down.
    pub fn shut_down(&self) {
        self.shutdown.cancel();
    }

    #[instrument(skip_all, fields(volume = request.volume_id))]
    async fn publish(
        &self,
        request: NodePublishVolumeRequest,
    ) -> Result<NodePublishVolumeResponse, Status> {
        check_request(&request.volume_id, &request.target_path)?;
        // The container runtime binds the target into the container again, and
        // that bind is writable unless the volume is read-only.
        if !request.readonly {
            return Err(Status::invalid_argument("the volume needs readOnly: true"));
        }
        let (attribute, store_paths) = store_paths(&request.volume_context, &system())?;
        let roots = (store_path::roots(store_paths.split_whitespace()))
            .map_err(|e| Status::invalid_argument(format!("{e:#}")))?;
        if roots.is_empty() {
            return Err(Status::invalid_argument(format!("{attribute} is empty")));
        }
        store::root_links(&roots).map_err(|e| Status::invalid_argument(format!("{e:#}")))?;

        let started = {
            let mut waiting = self.waiting.lock().unwrap();
            // Kubelet stops retrying when the pod goes away, or when a publish
            // fails for good, which would leave its entry here.
            waiting.retain(|_, started| started.elapsed() < Duration::from_hours(24));
            *(waiting.entry(request.volume_id.clone())).or_insert_with(Instant::now)
        };
        let mut ensuring = self.store.ensure(&roots);
        let done = tokio::select! {
            done = tokio::time::timeout(PUBLISH_WAIT, ensuring.done()) => done,
            () = self.shutdown.cancelled() => {
                return Err(Status::unavailable("the plugin is shutting down"));
            }
        };
        let names = match done {
            Ok(Ok(names)) => names,
            Ok(Err(e)) => {
                warn!("{e:#}");
                let code = if e.is::<store::TooBig>() {
                    tonic::Code::ResourceExhausted
                } else {
                    tonic::Code::Internal
                };
                return Err(Status::new(code, format!("{e:#}")));
            }
            Err(_) => {
                // Kubelet puts this message in the pod's events.
                let msg = format!("still {}", ensuring.progress());
                info!("{msg}");
                return Err(Status::unavailable(msg));
            }
        };

        let target = std::path::PathBuf::from(&request.target_path);
        let volume = request.volume_id.clone();
        let view_roots = roots.clone();
        self.for_volume(&request.volume_id, move |store| {
            let view = store.view(&volume, &view_roots, &names, &target, mounts::is_bound)?;
            mounts::publish(&view, &target)
        })
        .await?;
        self.waiting.lock().unwrap().remove(&request.volume_id);
        let elapsed_ms = started.elapsed().as_millis();
        info!(store_paths = %Names(&roots), elapsed_ms, "published");
        Ok(NodePublishVolumeResponse {})
    }

    #[instrument(skip_all, fields(volume = request.volume_id))]
    async fn unpublish(
        &self,
        request: NodeUnpublishVolumeRequest,
    ) -> Result<NodeUnpublishVolumeResponse, Status> {
        check_request(&request.volume_id, &request.target_path)?;
        let target = std::path::PathBuf::from(&request.target_path);
        let volume = request.volume_id.clone();
        let trash = self
            .for_volume(&request.volume_id, move |store| {
                mounts::unpublish(&target)?;
                store.drop_view(&volume)
            })
            .await?;
        // Deleting a big view is slow, so it happens after the hold on
        // collection ends.
        self.store.discard(trash);
        info!("unpublished");
        Ok(NodeUnpublishVolumeResponse {})
    }

    /// Skips the volume's turn, which a slow publish can hold for minutes.
    async fn stats(
        &self,
        request: NodeGetVolumeStatsRequest,
    ) -> Result<NodeGetVolumeStatsResponse, Status> {
        check_request(&request.volume_id, &request.volume_path)?;
        let missing = format!(
            "volume {} isn't published at {}",
            request.volume_id, request.volume_path
        );
        let store = self.store.clone();
        let usage =
            blocking(move || store.usage(&request.volume_id, Path::new(&request.volume_path)))
                .await?
                .ok_or_else(|| Status::not_found(missing))?;
        let full = |unit: volume_usage::Unit, used: u64| {
            let used = i64::try_from(used).unwrap_or(i64::MAX);
            // A read-only volume reports itself full, like a squashfs mount.
            VolumeUsage {
                available: 0,
                total: used,
                used,
                unit: unit.into(),
            }
        };
        Ok(NodeGetVolumeStatsResponse {
            usage: vec![
                full(volume_usage::Unit::Bytes, usage.bytes),
                full(volume_usage::Unit::Inodes, usage.inodes),
            ],
        })
    }

    /// Runs `f` on a blocking thread in the volume's turn, with collection held
    /// off. The turn and the hold last until `f` ends, even if kubelet gives up.
    async fn for_volume<T: Send + 'static>(
        &self,
        volume: &str,
        f: impl FnOnce(&Store) -> anyhow::Result<T> + Send + 'static,
    ) -> Result<T, Status> {
        let lock = {
            let mut volumes = self.volumes.lock().unwrap();
            // Otherwise the map keeps every volume there ever was.
            volumes.retain(|_, lock| Arc::strong_count(lock) > 1);
            volumes.entry(volume.to_owned()).or_default().clone()
        };
        let held = (lock.lock_owned().await, self.store.gc_guard().await);
        let store = self.store.clone();
        blocking(move || {
            let _held = held;
            f(&store)
        })
        .await
    }
}

/// Runs `f` on a blocking thread.
async fn blocking<T: Send + 'static>(
    f: impl FnOnce() -> anyhow::Result<T> + Send + 'static,
) -> Result<T, Status> {
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| Status::internal(e.to_string()))?
        .map_err(|e| Status::internal(format!("{e:#}")))
}

#[tonic::async_trait]
impl Identity for Plugin {
    async fn get_plugin_info(
        &self,
        _: Request<GetPluginInfoRequest>,
    ) -> Result<Response<GetPluginInfoResponse>, Status> {
        Ok(Response::new(GetPluginInfoResponse {
            name: DRIVER.into(),
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
        let call = self.store.metrics().publish();
        let result = self.publish(request.into_inner()).await;
        call.finish(&result);
        result.map(Response::new)
    }

    async fn node_unpublish_volume(
        &self,
        request: Request<NodeUnpublishVolumeRequest>,
    ) -> Result<Response<NodeUnpublishVolumeResponse>, Status> {
        let call = self.store.metrics().unpublish();
        let result = self.unpublish(request.into_inner()).await;
        call.finish(&result);
        result.map(Response::new)
    }
    async fn node_get_capabilities(
        &self,
        _: Request<NodeGetCapabilitiesRequest>,
    ) -> Result<Response<NodeGetCapabilitiesResponse>, Status> {
        let rpc = node_service_capability::Rpc {
            r#type: node_service_capability::rpc::Type::GetVolumeStats.into(),
        };
        Ok(Response::new(NodeGetCapabilitiesResponse {
            capabilities: vec![NodeServiceCapability {
                r#type: Some(node_service_capability::Type::Rpc(rpc)),
            }],
        }))
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
        request: Request<NodeGetVolumeStatsRequest>,
    ) -> Result<Response<NodeGetVolumeStatsResponse>, Status> {
        self.stats(request.into_inner()).await.map(Response::new)
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

fn store_paths<'a>(
    context: &'a HashMap<String, String>,
    system: &str,
) -> Result<(String, &'a str), Status> {
    store_path::for_system(context, system).ok_or_else(|| {
        let mut msg = format!("no {STORE_PATHS}.{system} or {STORE_PATHS} attribute");
        // Kubelet adds attributes under this prefix.
        let mut others: Vec<_> = (context.keys())
            .filter(|k| !k.starts_with("csi.storage.k8s.io/"))
            .map(String::as_str)
            .collect();
        if !others.is_empty() {
            others.sort_unstable();
            msg = format!("{msg}, only {}", others.join(", "));
        }
        Status::invalid_argument(msg)
    })
}

/// The volume ID names a file in the state dir. An empty target would leave
/// the mount in place while collection deletes its view.
fn check_request(volume_id: &str, target_path: &str) -> Result<(), Status> {
    if target_path.is_empty() {
        return Err(Status::invalid_argument("no target path"));
    }
    store::check_volume(volume_id).map_err(|e| Status::invalid_argument(format!("{e:#}")))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn picks_store_paths_by_system() {
        let context = |pairs: &[(&str, &str)]| -> HashMap<String, String> {
            (pairs.iter())
                .map(|&(k, v)| (k.to_owned(), v.to_owned()))
                .collect()
        };
        let both = context(&[
            ("storePaths", "/nix/store/a"),
            ("storePaths.aarch64-linux", "/nix/store/b"),
        ]);
        let (attribute, value) = store_paths(&both, "aarch64-linux").unwrap();
        assert_eq!(
            (attribute.as_str(), value),
            ("storePaths.aarch64-linux", "/nix/store/b")
        );
        let (attribute, value) = store_paths(&both, "x86_64-linux").unwrap();
        assert_eq!((attribute.as_str(), value), ("storePaths", "/nix/store/a"));

        let other = context(&[
            ("storePaths.aarch64-linux", "/nix/store/b"),
            ("csi.storage.k8s.io/ephemeral", "true"),
        ]);
        let e = store_paths(&other, "x86_64-linux").unwrap_err();
        assert_eq!(e.code(), tonic::Code::InvalidArgument);
        assert_eq!(
            e.message(),
            "no storePaths.x86_64-linux or storePaths attribute, only storePaths.aarch64-linux"
        );
        let e = store_paths(&context(&[]), "x86_64-linux").unwrap_err();
        assert_eq!(
            e.message(),
            "no storePaths.x86_64-linux or storePaths attribute"
        );
    }

    #[test]
    fn checks_requests() {
        assert!(check_request("v1", "/t").is_ok());
        for (volume, target) in [
            ("v1", ""),
            ("", "/t"),
            (".", "/t"),
            ("..", "/t"),
            ("a/b", "/t"),
        ] {
            let e = check_request(volume, target).unwrap_err();
            assert_eq!(
                e.code(),
                tonic::Code::InvalidArgument,
                "{volume:?} {target:?}"
            );
        }
    }
}
