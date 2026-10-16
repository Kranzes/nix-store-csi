# Deploying

## Install

The plugin needs:

- Kubernetes 1.30 or later
- Linux 6.2 or later on x86_64 or aarch64 nodes, with `landlock` in the
  kernel's `lsm=` list (`/sys/kernel/security/lsm`)
- a namespace that allows privileged pods, like `kube-system`
- `kubeletDir` set to kubelet's root directory, if it isn't `/var/lib/kubelet`
- disk space in `stateDir`, `/var/lib/nix-store-csi` by default, for the
  closures that pods use

```sh
helm install nix-store-csi oci://ghcr.io/kranzes/charts/nix-store-csi -n kube-system
```

[values.yaml](../deploy/helm/nix-store-csi/values.yaml) lists the chart's settings, and
[CLI.md](CLI.md) lists the plugin flags that they set.

The node plugin runs privileged. In a namespace other than `kube-system` that
enforces a Pod Security level, the level has to be `privileged`. Its volumes
are allowed at the restricted level, so pods that use them pass it with the
usual `securityContext`, as in [hello-pod.yaml](../deploy/hello-pod.yaml).

Mounted volumes keep working while a node's plugin restarts, as in a rollout.

[PODS.md](PODS.md) shows how pods use the driver.

## Running the plugin

The plugin unpacks NARs on one thread per CPU, up to `--max-substitution-jobs`
(16 by default). A CPU limit in `resources.limits` counts as whole CPUs,
so `2500m` gives 2 threads.

### Private caches

Credentials for private caches go in a netrc file in a Secret, the format Nix
uses for its `netrc-file`. The plugin reads the file for every request, so
changes to the Secret apply without a restart. A private cache also needs its
key among the trusted ones:

```sh
kubectl -n kube-system create secret generic nix-store-csi-netrc \
  --from-literal=netrc="machine cache.example.com login user password secret"
```

```yaml
# values.yaml
substituters:
  - https://cache.example.com
  - https://cache.nixos.org
trustedPublicKeys:
  - cache.example.com-1:<key>
  - cache.nixos.org-1:6NCHdD59X431o0gWypbMrAURkbJ16ZPMQFGspcDShjY=
netrcSecret: nix-store-csi-netrc
```

```sh
helm upgrade --install nix-store-csi oci://ghcr.io/kranzes/charts/nix-store-csi -n kube-system -f values.yaml
```

### S3

An `s3://` substituter reads a bucket like Nix's S3 store does, with its
`region`, `endpoint` and `addressing-style` settings. The plugin takes its
credentials from EKS Pod Identity, IRSA, the keys in `s3CredentialsSecret`, or
the node's instance role. With Pod Identity, give the plugin's ServiceAccount,
which the chart names after the release, a role that can `s3:GetObject` on
the bucket. Without `s3:ListBucket`, S3 answers 403 for a missing file, which
the plugin treats like a 404.

```yaml
# values.yaml
substituters:
  - s3://my-cache?region=eu-west-1
  - https://cache.nixos.org
trustedPublicKeys:
  - my-cache-1:<key>
  - cache.nixos.org-1:6NCHdD59X431o0gWypbMrAURkbJ16ZPMQFGspcDShjY=
```

#### GCS

For GCS, set `endpoint` to `https://storage.googleapis.com`. Without keys in
`s3CredentialsSecret`, the plugin reads the bucket with a token from GKE
Workload Identity. Grant `roles/storage.objectViewer` on the bucket to the plugin's
ServiceAccount, as the principal
`principal://iam.googleapis.com/projects/<number>/locations/global/workloadIdentityPools/<project>.svc.id.goog/subject/ns/kube-system/sa/nix-store-csi`.
Workload Identity doesn't serve pods on the host network, so `hostNetwork`
must stay `false`.

```yaml
# values.yaml
substituters:
  - s3://my-cache?endpoint=https://storage.googleapis.com
  - https://cache.nixos.org
trustedPublicKeys:
  - my-cache-1:<key>
  - cache.nixos.org-1:6NCHdD59X431o0gWypbMrAURkbJ16ZPMQFGspcDShjY=
```

#### Other S3 services

Services like Cloudflare R2 take an `endpoint`, the `region` they sign with,
and keys. `s3CredentialsSecret` names a Secret with `AWS_ACCESS_KEY_ID` and
`AWS_SECRET_ACCESS_KEY`, which the plugin gets as environment variables.

```yaml
# values.yaml
substituters:
  - s3://my-cache?endpoint=https://<account-id>.r2.cloudflarestorage.com&region=auto
  - https://cache.nixos.org
trustedPublicKeys:
  - my-cache-1:<key>
  - cache.nixos.org-1:6NCHdD59X431o0gWypbMrAURkbJ16ZPMQFGspcDShjY=
s3CredentialsSecret: r2-credentials
```

### Prefetching

To have closures on the nodes before pods need them, list their store paths
in the `prefetch` value:

```yaml
# values.yaml
prefetch:
  storePaths.x86_64-linux: /nix/store/5z2yp3ysx8476c8g5w25b0smlgkjvaq3-hello-2.12.3
  storePaths.aarch64-linux: /nix/store/0xy6jlccm8kfpwmhkxabz3638lchyi74-hello-2.12.3
```

The chart puts the list in a ConfigMap. Kubelet updates the mounted list
within about a minute and a half, and the plugin starts fetching a new list
as soon as it changes, without a restart.

- Garbage collection keeps prefetched closures even when `--ensure-free` needs
  the space, so keep the list small enough to fit next to what pods use.
- If the list can't be read or fetched, the plugin logs a warning and tries
  again a minute later. Until then it keeps the closures it last fetched.

For a rollout:

1. Set `prefetch` to the new version's store paths and upgrade the chart.
   With [manifests.nix](../deploy/manifests.nix), pass its `prefetch` output to
   `helm upgrade` with `-f`. It replaces the lists for the systems its store
   paths are built for.
2. Wait until every node plugin holds the new list. The chart puts a
   `revision` of the list in its ConfigMap. With `metrics.enabled`, the
   chart's notes print it and a Prometheus query that returns nothing once
   every node plugin holds that revision:

   ```promql
   nix_store_csi_build_info unless on (instance) nix_store_csi_prefetched{revision="<revision>"} == 1
   ```

   The query can't see a node plugin that Prometheus doesn't scrape, so check
   that every node runs one.

   Without metrics, wait until each node plugin logs `prefetched` with those
   store paths.
3. Update the Deployment. Its new pods start without waiting for a fetch.

Step 2 is optional. If you update the Deployment right away, each new pod's
node fetches the closure when it mounts the volume, and a path that the
prefetch is already fetching downloads only once. Waiting has two benefits. If
a store path is missing from the caches or has no trusted signature, the
prefetch fails while the old pods still run, and no new pods wait in
`ContainerCreating`. Also, no step of the rolling update waits for a fetch.

### Node pools

Every node of a system gets the same `prefetch` list. In a cluster whose nodes
run different workloads, like a pool of GPU nodes for training and a pool for
web services, every node would hold every pool's closures. `nodePools` gives
each pool its own list:

```yaml
# values.yaml
prefetch:
  storePaths.x86_64-linux: /nix/store/<hash>-api
nodePools:
  gpu:
    nodeSelector:
      karpenter.sh/nodepool: gpu
    prefetch:
      storePaths.x86_64-linux: /nix/store/<hash>-trainer
```

- Each pool gets a DaemonSet, `<release>-node-<pool>`, on the nodes that match
  both its `nodeSelector` and the top-level one, and a ConfigMap,
  `<release>-prefetch-<pool>`.
- A pool's list replaces `prefetch`. Its nodes don't fetch the top-level list,
  and a pool without `prefetch` prefetches nothing. `prefetch` is the list for
  the nodes outside every pool.
- A closure that every node needs, like a log shipper, goes in `prefetch` and
  in each pool's list. The chart doesn't merge the lists. A node reads
  `storePaths` only if its list has no key for its system, so a pool's
  `storePaths.x86_64-linux` would hide a top-level `storePaths` from the
  pool's x86 nodes.
- A node that matches more than one pool goes to the first pool by name.
- If a node's labels move it to another pool, Kubernetes replaces its plugin
  with the other pool's. The node store stays, and mounted volumes keep
  working.

Each list has its own revision. The chart's PodMonitor puts the pool's name in
a `pool` label, which is empty outside the pools. The chart's notes print a
query for each list:

```promql
nix_store_csi_build_info{pool="gpu"} unless on (instance) nix_store_csi_prefetched{revision="<revision>"} == 1
```

Without the PodMonitor, copy the `nix-store-csi/pool` label of the plugin's
pods into the metrics in your own scrape config.

### Security

The CSIDriver is cluster-scoped, and every Pod Security level allows its
volumes. So anyone who can create a pod in any namespace can mount the closure
of any store path that the configured caches serve. That includes private
caches that the plugin reaches with the netrc Secret or a service account
token.

Pods can ask for store paths whose closures are any size. A node fetches the
closure of every pod it runs, and the fetch goes on after the pod is gone.
Collection can't delete store paths that a fetch or a mounted view holds, so
pods with big closures can fill the state dir's disk.

`maxClosureSize`, like `20G`, limits the NAR size of each volume's closure.
The plugin refuses a bigger closure before it fetches any of it. The pod stays
in `ContainerCreating`, and its events show the limit and the closure's size,
which `nix path-info -S` also prints. Prefetched closures don't count towards
it, so prefetching a big closure lets pods use it. Once its store paths leave
`prefetch`, a closure counts again. During a rollout, keep the old version's
store paths in `prefetch` for as long as you may roll back. Many pods with
different closures can still fill the disk.

A pod gets only its own closure, on a read-only mount.

To allow the driver's volumes only in some namespaces, list them in
`admissionPolicy.allowedNamespaces`:

```yaml
# values.yaml
admissionPolicy:
  allowedNamespaces:
    - ci
    - web
```

The chart then adds a ValidatingAdmissionPolicy. It checks pods, so the API
server still accepts a Deployment in another namespace, and its ReplicaSet's
events show why it has no pods.

### Image signatures

CI signs both images with Sigstore. Each image's index has SLSA build
provenance, and each platform's image has a CycloneDX SBOM. The signer of a
release is the CI workflow at its tag,
`https://github.com/Kranzes/nix-store-csi/.github/workflows/ci.yml@refs/tags/v<version>`.
The signatures cover the images, not the store paths that the driver mounts
in pods, which the plugin checks against `trustedPublicKeys`.

To check an image, and to save the SBOM of its amd64 image:

```sh
image=ghcr.io/kranzes/nix-store-csi:<version>
gh attestation verify "oci://$image" --repo Kranzes/nix-store-csi
digest=$(regctl image digest --platform linux/amd64 "$image")
gh attestation verify "oci://${image%:*}@$digest" --repo Kranzes/nix-store-csi \
  --predicate-type https://cyclonedx.org/bom \
  --format json --jq '.[0].verificationResult.statement.predicate' > nix-store-csi.cdx.json
```

Admission controllers that verify Sigstore bundles, like
[Kyverno](https://kyverno.io/docs/policy-types/cluster-policy/verify-images/sigstore/)
or the Sigstore
[policy-controller](https://docs.sigstore.dev/policy-controller/overview/),
can admit only signed releases. Their policy checks the signer above and the
SLSA provenance on the image's index. With policy-controller, add this
ClusterImagePolicy:

```yaml
apiVersion: policy.sigstore.dev/v1alpha1
kind: ClusterImagePolicy
metadata:
  name: nix-store-csi
spec:
  images:
    - glob: ghcr.io/kranzes/nix-store-csi**
  authorities:
    - keyless:
        url: https://fulcio.sigstore.dev
        identities:
          - issuer: https://token.actions.githubusercontent.com
            subjectRegExp: ^https://github\.com/Kranzes/nix-store-csi/\.github/workflows/ci\.yml@refs/tags/v
      ctlog:
        url: https://rekor.sigstore.dev
      signatureFormat: bundle
      attestations:
        - name: provenance
          predicateType: https://slsa.dev/provenance/v1
```

Then label the plugin's namespace, and those of pods that use the runner
image, with `policy.sigstore.dev/include=true`. Images there that no policy
matches are rejected, so the sidecars in `registrar.image` and
`livenessProbe.image` need a policy too, like one for
[Kubernetes' signatures](https://kubernetes.io/docs/tasks/administer-cluster/verify-signed-artifacts/).
The controller pins each image to the digest it checked, so `values.yaml` and
pod specs can keep tags.

### Metrics

With `metrics.enabled=true`, the node plugin serves Prometheus metrics at
`/metrics` on port 9809 (`metrics.port`).

- `metrics.podMonitor.enabled=true` adds a PodMonitor for the Prometheus
  Operator. `metrics.podMonitor.labels` adds labels to it.
- `metrics.grafanaDashboard.enabled=true` adds a ConfigMap with a
  [Grafana dashboard](../deploy/helm/nix-store-csi/dashboards/nix-store-csi.json) for
  Grafana's dashboard sidecar.
- `metrics.prometheusRule.enabled=true` adds a PrometheusRule with the alerts
  below. `metrics.prometheusRule.labels` adds labels to it, and
  `metrics.prometheusRule.alertLabels` to each alert.

Every metric starts with `nix_store_csi_`:

| Metric | Type | Labels |
|---|---|---|
| `publish_duration_seconds` | histogram | `grpc_code` |
| `unpublish_duration_seconds` | histogram | `grpc_code` |
| `closures_in_flight` | gauge | |
| `nar_fetch_duration_seconds` | histogram | `cache`, `result` |
| `nar_fetched_bytes_total` | counter | `cache` |
| `nar_fetches_in_flight` | gauge | |
| `cache_paused` | gauge | `cache` |
| `store_paths` | gauge | |
| `store_size_bytes` | gauge | |
| `unsynced_paths` | gauge | |
| `failed_syncs_total` | counter | |
| `views` | gauge | |
| `free_bytes` | gauge | |
| `free_inodes` | gauge | |
| `ensure_free_bytes` | gauge | |
| `ensure_free_inodes` | gauge | |
| `gc_duration_seconds` | histogram | `result` |
| `gc_deleted_paths_total` | counter | |
| `gc_freed_bytes_total` | counter | |
| `prefetched` | gauge | `revision` |
| `build_info` | gauge | `version` |

A publish that is still fetching after 90 seconds counts as `Unavailable`, and
kubelet's retry counts again. `nar_fetched_bytes_total`,
`gc_freed_bytes_total` and `store_size_bytes` count uncompressed NAR sizes.
`closures_in_flight` counts closures still being fetched, including those
whose publish already returned `Unavailable`. `free_bytes` and `free_inodes`
are free on the state dir's filesystem, and `ensure_free_bytes` and
`ensure_free_inodes` are what `--ensure-free` keeps free there. The inode
gauges are missing on a filesystem with no limit on inodes, like btrfs. A
failed sync deletes the store paths it covered and counts in
`failed_syncs_total`.
`prefetched` is 1 once the node store holds the closures of that revision of
the `prefetch` list for the node's system.

Each alert in `metrics.prometheusRule.alerts` has `enabled`, `for` and
`severity`:

| Alert | Fires when |
|---|---|
| `NixStoreCsiDown` | Prometheus can't scrape a plugin. It needs the PodMonitor. |
| `NixStoreCsiPublishErrors` | Publishes on a node fail with `Internal`, like for a store path that no cache has. |
| `NixStoreCsiCacheUnreachable` | A node skips a cache for most of 15 minutes after failed requests. |
| `NixStoreCsiNarFetchErrors` | More than `ratio`, 10% by default, of the NAR fetches from a cache fail. |
| `NixStoreCsiSyncStuck` | Fetched store objects wait for a sync while no fetch runs. |
| `NixStoreCsiSyncErrors` | A sync failed in the last hour. |
| `NixStoreCsiGcErrors` | Collections fail. |
| `NixStoreCsiLowSpace` | The filesystem has less free space or inodes than `--ensure-free` keeps, so collection deletes store paths used within `--keep-recent`. |
| `NixStoreCsiPrefetchBehind` | A node doesn't hold the current revision of its prefetch list. Set `for` above the time a node takes to fetch the list. With `nodePools` it needs the `pool` label. |
