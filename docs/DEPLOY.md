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

To pull the images from a private registry, list the Secrets with its
credentials in `imagePullSecrets`.

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

Credentials for private caches go in a netrc file, the format Nix uses for its
`netrc-file`, under the `netrc` key of a Secret in the plugin's namespace.
Create it however you manage secrets. The plugin reads the file for every
request, so changes to the Secret apply without a restart. A private cache
also needs its key among the trusted ones:

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: nix-store-csi-netrc
  namespace: kube-system
stringData:
  netrc: |
    machine cache.example.com password <token>
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

### S3

An `s3://` substituter reads a bucket like Nix's S3 store does, with its
`region`, `endpoint` and `addressing-style` settings. The services below
differ in those settings and in where the plugin gets credentials.

For a public bucket, set `AWS_SKIP_SIGNATURE=true`, for example in
`s3CredentialsSecret`. The plugin then reads every `s3://` cache without
credentials.

#### AWS

The plugin takes its credentials from EKS Pod Identity, IRSA, the keys in
`s3CredentialsSecret`, or the node's instance role. With Pod Identity or IRSA,
give the plugin's ServiceAccount, which the chart names after the release, a
role that can `s3:GetObject` on the bucket. For IRSA, put the role's ARN in
`serviceAccount.annotations`, as below. Without `s3:ListBucket`, S3 answers 403
`AccessDenied` for a missing file, which the plugin treats like a 404. Other
403 codes, like `SignatureDoesNotMatch`, are errors, so the plugin skips the
file for a minute, and the whole cache once a second file fails.

```yaml
# values.yaml
substituters:
  - s3://my-cache?region=eu-west-1
  - https://cache.nixos.org
trustedPublicKeys:
  - my-cache-1:<key>
  - cache.nixos.org-1:6NCHdD59X431o0gWypbMrAURkbJ16ZPMQFGspcDShjY=
serviceAccount:
  annotations:
    eks.amazonaws.com/role-arn: arn:aws:iam::<account-id>:role/<role>
```

#### GCS

For GCS, set `endpoint` to `https://storage.googleapis.com`. Without keys in
`s3CredentialsSecret`, the plugin reads the bucket with a token from GKE
Workload Identity. Grant `roles/storage.objectViewer` on the bucket to the
plugin's ServiceAccount, as the principal
`principal://iam.googleapis.com/projects/<number>/locations/global/workloadIdentityPools/<project>.svc.id.goog/subject/ns/<namespace>/sa/<release>`,
since the chart names the ServiceAccount after the release. Workload Identity
doesn't serve pods on the host network, so `hostNetwork` must stay `false`.

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

Each key is a list, and a key may add a name after the system, as in
`storePaths.<system>.<name>`. Helm merges `prefetch` across values files, so
teams can each keep their own key without overwriting the others':

```yaml
# web.yaml
prefetch:
  storePaths.x86_64-linux.web: /nix/store/<hash>-web
```

```yaml
# batch.yaml
prefetch:
  storePaths.x86_64-linux.batch: /nix/store/<hash>-batch
```

A node prefetches the store paths of every key for its system, or if there is
none, of `storePaths`. It fetches each key's list on its own, so a list with a
typo or a store path that no cache has doesn't stop the others.

Owners who write their keys outside Helm, like a CI job with
`kubectl patch`, need a ConfigMap that the chart doesn't render. Name it in
`prefetchConfigMap` in place of `prefetch`:

```yaml
# values.yaml
prefetchConfigMap: prefetch-lists
```

It has to be in the release's namespace. The plugin starts before it exists
and reads its lists once it does. The chart doesn't know these lists, so only
the alert rule for lists that a node has read but doesn't hold covers them.

- Garbage collection keeps prefetched closures even when `--ensure-free` needs
  the space, so keep the lists small enough to fit next to what pods use.
- If the plugin can't read or fetch a list, it logs a warning that names the
  list's file and tries again a minute later. Until then it keeps the closures
  it last fetched for that list.

For a rollout:

1. Set `prefetch` to the new version's store paths and upgrade the chart.
   With [manifests.nix](../deploy/manifests.nix), pass its `prefetch` output
   as a values file. It replaces the lists for the systems its store paths are
   built for.
2. Wait until every node plugin holds the new list. With `metrics.enabled`,
   the chart's notes print this Prometheus query, which returns nothing once
   every node plugin that reads a list holds it:

   ```promql
   nix_store_csi_build_info{system="x86_64-linux"} unless on (instance) nix_store_csi_prefetched{list="web", hash="<hash>"} == 1
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

Each node plugin reports a series for each list it reads. Its `list` label is
the key's name after the system, empty for `storePaths.<system>` and
`storePaths`. Its `hash` label is the first 12 hex digits of the SHA-256 of
the key's value, so an owner who sets its key outside the chart can wait for
it too:

```sh
kubectl -n <namespace> get configmap <release>-prefetch \
  -o jsonpath='{.data.storePaths\.x86_64-linux\.web}' | sha256sum | head -c 12
```

`build_info`'s `system` label keeps the query to the nodes that read the
list. For `storePaths`, select the systems without keys of their own, as in
`system!~"x86_64-linux|aarch64-linux"`.

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
  and a pool without `prefetch` or `prefetchConfigMap` prefetches nothing.
  `prefetch` is the list for the nodes outside every pool.
- A closure that every node needs, like a log shipper, goes in `prefetch` and
  in each pool's list. The chart doesn't merge the lists. A node reads
  `storePaths` only if its list has no key for its system, so a pool's
  `storePaths.x86_64-linux` would hide a top-level `storePaths` from the
  pool's x86 nodes.
- A pool's `maxStoreSize` replaces the top-level one on its nodes, and an
  empty value turns the limit off for the pool. A percentage already fits
  pools with different disks.
- A node that matches more than one pool goes to the first pool by name.
- If a node's labels move it to another pool, Kubernetes replaces its plugin
  with the other pool's. The node store stays, and mounted volumes keep
  working.

The chart's PodMonitor puts the pool's name in a `pool` label, which is empty
outside the pools, and the chart's notes add it to the query:

```promql
nix_store_csi_build_info{pool="gpu", system="x86_64-linux"} unless on (instance) nix_store_csi_prefetched{list="", hash="<hash>"} == 1
```

Without the PodMonitor, copy the `nix-store-csi/pool` label of the plugin's
pods into the metrics in your own scrape config.

### Scheduling by closure

kube-scheduler's ImageLocality plugin prefers nodes that already have a pod's
images. With `locality.enabled`, the chart runs an extender that does the same
for closures. It scores each node by the share of the pod's closure, by NAR
size, that its node store holds.

```yaml
# values.yaml
locality:
  enabled: true
```

The extender reads `/store-paths` from every node plugin every 15 seconds. The
chart turns on the plugins' metrics port and `--serve-paths` for it, and adds
a headless Service, `<release>-plugins`, that lists them. A plugin whose node
store has the same store paths as at the last read answers 304 without a
listing. The extender holds each node's store paths in memory, so it needs
more memory with more nodes and bigger node stores. It resolves closures from
the plugins' caches, with the same keys and credentials. On a cluster that
taints all its nodes, `locality.nodeSelector` and `locality.tolerations` place
the extender's pod.

A node scores 0 until the extender reads its plugin, and again once its plugin
misses 3 reads in a row. A store path that a node fetches counts from the next
read, within 15 seconds. A node that just joined is then like one with a
`PreferNoSchedule` taint for each closure it lacks. Pods go to nodes that hold
their closures while those have room, and the new node's scores rise as it
fetches and prefetches.

A pod scores 0 on every node until the extender has resolved the closure of
its store paths. The first request that lists a store path starts resolving it
in the background, so the scheduler never waits for the caches. Resolving
reads a narinfo for each store path. From cache.nixos.org that takes about a
second for a few hundred store paths, and two for 1,700. Pods scheduled in the
meantime, like the first replicas of a new version, get no preference.
`nix-store-csi unpack --dry-run` resolves a closure the same way, so it shows
how long yours take. If resolving fails, pods with that store path score 0 for
a minute, until a request tries again.

kube-scheduler reads extenders from its configuration file. It usually runs
without cluster DNS, so the URL takes the cluster IP of the
`<release>-locality` Service, which the chart's notes print a command for:

```yaml
apiVersion: kubescheduler.config.k8s.io/v1
kind: KubeSchedulerConfiguration
extenders:
  - urlPrefix: http://<cluster IP>:8080
    prioritizeVerb: prioritize
    weight: 1
    nodeCacheCapable: true
    httpTimeout: 1s
    ignorable: true
```

The extender scores from 0 to 10. kube-scheduler multiplies the score by 10
and by `weight`, so `weight: 1` counts as much as ImageLocality.
`nodeCacheCapable` makes it send node names instead of whole Node objects.
kube-scheduler schedules one pod at a time, so `httpTimeout` limits how long a
slow extender holds up every pod after it. The default is 5 seconds. The
extender answers from memory and never waits for the caches. With `ignorable`,
a failed or slow extender doesn't fail scheduling, and the pod is scheduled
without its scores.

Hosted control planes like EKS, GKE and AKS don't let you configure the
default scheduler. There, pods need a second scheduler that they name in
`schedulerName`, or Volcano. Volcano's `extender` plugin scores from 0 to 100:

```yaml
- plugins:
    - name: extender
      arguments:
        extender.urlPrefix: http://<release>-locality.<namespace>.svc:8080/volcano
        extender.prioritizeVerb: prioritize
        extender.httpTimeout: 5s
        extender.ignorable: true
```

Volcano sends every pod on each node with the request, so its default timeout
of 1 second can be too short. Without `extender.ignorable`, a failed request
leaves the task unscheduled in that scheduling session. Volcano adds the score
to the other plugins' without a weight, so the weights of plugins like
`binpack` and `nodeorder` set how much it counts. It scores a pending task
again in every session, so a pod that waits for resources gets its scores
once its closure resolves. `extender.managedResources`, like
`[nvidia.com/gpu]`, sends only the tasks that request those resources.

### Security

The CSIDriver is cluster-scoped, and every Pod Security level allows its
volumes. So anyone who can create a pod in any namespace can mount the closure
of any store path that the configured caches serve. That includes private
caches that the plugin reaches with the netrc Secret or a service account
token.

Knowing a store path is enough to mount its closure. `/store-paths` lists
every store path on a node to anyone who can reach the metrics port, so
`--serve-paths` is off by default. `locality.enabled` turns it on.

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

`maxStoreSize`, like `200G` or `50%` of the state dir's filesystem, limits the
NAR size of what garbage collection can't delete: the closures of mounted
volumes, of fetches under way and of prefetched store paths. The plugin refuses
a volume whose closure would take that size over the limit before it fetches any
of it. The pod's events show the limit, that size and what the closure adds. A
closure that the node store already has always passes, since it takes no more
disk space. Mounting it still makes it uncollectable if it was only recently
used, so that size can go over the limit. Until volumes unmount, the plugin then
refuses every volume that needs a fetch. The `uncollectable_bytes` metric shows
that size on each node.

The filesystem is usually shared with kubelet and other caches, like the
container runtime's images, so choose a percentage with them in mind.

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
| `publish_duration_seconds` | histogram | `grpc_code`, `fetched` |
| `unpublish_duration_seconds` | histogram | `grpc_code` |
| `closures_in_flight` | gauge | |
| `nar_fetch_duration_seconds` | histogram | `cache`, `result` |
| `nar_fetched_bytes_total` | counter | `cache` |
| `nar_fetches_in_flight` | gauge | |
| `cache_paused` | gauge | `cache` |
| `store_paths` | gauge | |
| `store_size_bytes` | gauge | |
| `uncollectable_bytes` | gauge | |
| `max_store_size_bytes` | gauge | |
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
| `prefetched` | gauge | `list`, `hash` |
| `build_info` | gauge | `version`, `system` |

A publish that is still fetching after 90 seconds counts as `Unavailable`, and
kubelet's retry counts again. `fetched` is `true` for a publish that waited
for a fetch or retried after `Unavailable`, and `false` for one whose closure
the node store already had. `nar_fetched_bytes_total`,
`gc_freed_bytes_total`, `store_size_bytes` and `uncollectable_bytes` count
uncompressed NAR sizes. `uncollectable_bytes` is what `maxStoreSize` limits,
and `max_store_size_bytes` is the limit, missing when there is none. A
percentage counts against the filesystem's size at the time.
`closures_in_flight` counts closures still being fetched, including those
whose publish already returned `Unavailable`. `free_bytes` and `free_inodes`
are free on the state dir's filesystem, and `ensure_free_bytes` and
`ensure_free_inodes` are what `--ensure-free` keeps free there. The inode
gauges are missing on a filesystem with no limit on inodes, like btrfs. A
failed sync deletes the store paths it covered and counts in
`failed_syncs_total`.
`prefetched` is 1 once the node store holds the closure of the list with that
name and hash. `system` is the node's Nix system.

With `--serve-paths` in `extraArgs`, or with `locality.enabled`, the same port
also serves `/store-paths`, the store paths in the node store, one on each
line. Its `ETag` changes when the node store gains or loses a store path, and
a request whose `If-None-Match` has the current one gets 304 without a
listing.

With `locality.enabled`, the locality extender serves its own metrics at
`/metrics` on port 8080 (`locality.port`), and `metrics.podMonitor.enabled`
adds a PodMonitor for it:

| Metric | Type | Labels |
|---|---|---|
| `locality_scrapes_total` | counter | `result` |
| `locality_nodes` | gauge | |
| `locality_closure_resolves_total` | counter | `result` |
| `locality_prioritize_duration_seconds` | histogram | `scheduler`, `result` |

`locality_scrapes_total` counts reads of the plugins' `/store-paths`, those
answered with 304 too, and failed lookups of `<release>-plugins`. `locality_nodes` counts the nodes whose
plugin the extender read within the last 3 intervals.

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
| `NixStoreCsiStoreNearLimit` | What garbage collection can't delete is over `ratio`, 90% by default, of `maxStoreSize`, so publishes that need a fetch may fail. |
| `NixStoreCsiPrefetchBehind` | A node doesn't hold the current version of a prefetch list that it reads. One rule fires for a list that a node has read but doesn't hold. For each list that the chart renders, another fires while a node hasn't read its current version. A list that others add has no such rule, so no rule sees a node still on its old hash. Set `for` above the time a node takes to fetch a list. With `nodePools`, the rules for the chart's lists need the `pool` label. |
| `NixStoreCsiLocalityDown` | Prometheus can't scrape the locality extender, or it isn't running. It needs the PodMonitor. |
| `NixStoreCsiLocalityScrapeErrors` | More than `ratio`, 10% by default, of the locality extender's reads of the node plugins fail. |
| `NixStoreCsiLocalityResolveErrors` | The locality extender fails to resolve closures from the caches for 15 minutes. |
