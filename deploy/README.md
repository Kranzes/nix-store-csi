# Deploying

## Install

The plugin needs:

- Kubernetes 1.30 or later
- Linux nodes on x86_64 or aarch64
- a namespace that allows privileged pods, like `kube-system`
- `kubeletDir` set to kubelet's root directory, if it isn't `/var/lib/kubelet`
- disk space in `stateDir`, `/var/lib/nix-store-csi` by default, for the
  closures that pods use

```sh
helm install nix-store-csi oci://ghcr.io/kranzes/charts/nix-store-csi -n kube-system
```

[values.yaml](helm/nix-store-csi/values.yaml) lists the chart's settings, and
[CLI.md](../docs/CLI.md) lists the plugin flags that they set.

The node plugin runs privileged. In a namespace other than `kube-system` that
enforces a Pod Security level, the level has to be `privileged`. Its volumes
are allowed at the restricted level, so pods that use them pass it with the
usual `securityContext`, as in the example below.

Mounted volumes keep working while a node's plugin restarts, as in a rollout.

## Example

[hello-pod.yaml](hello-pod.yaml) runs GNU hello on an amd64 or arm64 node:

```sh
kubectl apply -f https://raw.githubusercontent.com/Kranzes/nix-store-csi/master/deploy/hello-pod.yaml
kubectl logs hello
```

A pod stays in `ContainerCreating` until its closure is on the node. Past 90
seconds, its events show how much has arrived.

## Pod manifests

### Injecting store paths into manifests

A nixpkgs update often changes store paths, so paths copied into manifests by
hand go stale. Instead, write a variable like
`${NIX_STORE_PATH_HELLO}` where a path goes and let Nix fill it in. In a pod
for one system, that changes two lines:

```yaml
args: [${NIX_STORE_PATH_HELLO}/bin/hello]
# ...
volumeAttributes:
  storePaths: ${NIX_STORE_PATH_HELLO}
```

[manifests.nix](manifests.nix) fills in the variables. Each name in its
`storePaths` set becomes a `NIX_STORE_PATH_` variable.

- `unsafeDiscardStringContext` drops the derivation's dependency on the
  packages, so the build neither builds nor fetches them. The nodes fetch
  them, so they have to be in a cache the plugin uses.
- `envsubst` replaces only `NIX_STORE_PATH_` variables. Others in the
  manifests, like those in a shell script or Nix's own `NIX_PATH`, stay as
  they are. The build fails on a `NIX_STORE_PATH_` variable that `storePaths`
  doesn't set.
- The store paths match the system of the `pkgs` that calls the derivation.
  Pin pods to that architecture with a `nodeSelector` on
  `kubernetes.io/arch`, or list store paths for each system as in
  [Multiple architectures](#multiple-architectures).

CI can build the derivation and commit the result to a branch that Flux or
Argo CD syncs. The same variables work in Helm values files and other
text that ends up in a pod spec.

The derivation's `prefetch` output is a values file with the chart's
`prefetch` value for the same store paths. Apply it in a commit or sync of its
own, and the manifests once the nodes hold it, as in
[Prefetching](#prefetching).

### Multiple architectures

One manifest can serve nodes of multiple architectures. Add a
`storePaths.<system>` attribute for each Nix system. A node uses the one for
its system and falls back to `storePaths`. The store paths differ per system,
so `args` can't name one. It uses `/nix/store/.roots/<name>` instead, a link
that the plugin adds for each listed store path, named without the hash.
[hello-pod.yaml](hello-pod.yaml) does this:

```yaml
spec:
  affinity:
    nodeAffinity:
      requiredDuringSchedulingIgnoredDuringExecution:
        nodeSelectorTerms:
          - matchExpressions:
              - key: kubernetes.io/arch
                operator: In
                values: [amd64, arm64]
  containers:
    - name: hello
      args: [/nix/store/.roots/hello-2.12.3/bin/hello]
      # ...
  volumes:
    - name: nix
      csi:
        driver: nix-store-csi
        readOnly: true
        volumeAttributes:
          storePaths.x86_64-linux: /nix/store/5z2yp3ysx8476c8g5w25b0smlgkjvaq3-hello-2.12.3
          storePaths.aarch64-linux: /nix/store/0xy6jlccm8kfpwmhkxabz3638lchyi74-hello-2.12.3
```

- The affinity keeps the pod off nodes that have no store paths for their
  system. A volume can't affect scheduling. Without the affinity, a pod on
  such a node stays in `ContainerCreating`, and its events show the error.
- Two store paths in one list can't have the same name, since they would need
  the same link.
- The name has the version in it, so `args` changes when the version does. To
  inject the paths, add a variable for each system, like
  `HELLO_AMD64 = nixpkgs.legacyPackages.x86_64-linux.hello`, and get the
  link's name from `hello.name`.

### Multiple programs on `PATH`

List one
[`buildEnv`](https://nixos.org/manual/nixpkgs/unstable/#sec-buildEnv) of the
packages, in [manifests.nix](manifests.nix):

```nix
TOOLS = buildEnv {
  name = "tools";
  paths = [ hello curl ];
};
```

and put its `bin` on `PATH`:

```yaml
env:
  - name: PATH
    value: ${NIX_STORE_PATH_TOOLS}/bin
# ...
volumeAttributes:
  storePaths: ${NIX_STORE_PATH_TOOLS}
```

### Your own image

The example runs in the runner image, which has tini as its entrypoint, an
empty `/nix/store`, users, `/tmp` and nixpkgs' CA certificates, and runs as
nobody. Any other image works if it meets these requirements:

- Nothing in the image may need its own `/nix/store`, since the volume hides
  it. An image without `/nix/store` is fine, because the container runtime
  creates the mount point.
- An init is optional if the program exits on SIGTERM and either starts no
  child processes or reaps the ones it starts, as most servers do. Otherwise
  deleting the pod waits out its grace period, and finished children pile up
  as zombies. Then use the image's own init, or run tini from the closure. Put
  it in `command`, which replaces the image's entrypoint, and in `storePaths`:

  ```yaml
  command: [${NIX_STORE_PATH_TINI}/bin/tini, -s, --]
  args: [${NIX_STORE_PATH_HELLO}/bin/hello]
  # ...
  volumeAttributes:
    storePaths: ${NIX_STORE_PATH_TINI} ${NIX_STORE_PATH_HELLO}
  ```

  Add `TINI = tini` to `storePaths` in [manifests.nix](manifests.nix).
- The image provides what the program expects from its environment, like users
  in `/etc/passwd`, `/tmp` and CA certificates.

Keep store paths out of the image, and the program's store path only in `args`
and `storePaths`. The image then doesn't change when the program does. The
runner image is built this way from [base-image.nix](base-image.nix), which
copies what it needs instead of linking into the store. To add a company CA,
build your own with `extraCertificates = [ ./company-ca.pem ];`.

To run an existing image built with `dockerTools.buildLayeredImage`, set
`includeStorePaths = false`. The image then holds only links into `/nix/store`,
and `storePaths` has to list the store paths it links to. Rebuild the image when
those paths change.

## Running the plugin

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
within about a minute, and the plugin reads it every minute, so a new list
starts fetching within two minutes, without a restart.

- Garbage collection keeps prefetched closures even when `--ensure-free` needs
  the space, so keep the list small enough to fit next to what pods use.
- If the list can't be read or fetched, the plugin logs a warning and tries
  again a minute later. Until then it keeps the closures it last fetched.

For a rollout:

1. Set `prefetch` to the new version's store paths and upgrade the chart.
   With [manifests.nix](manifests.nix), pass its `prefetch` output to
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

### Metrics

With `metrics.enabled=true`, the node plugin serves Prometheus metrics at
`/metrics` on port 9809 (`metrics.port`).

- `metrics.podMonitor.enabled=true` adds a PodMonitor for the Prometheus
  Operator. `metrics.podMonitor.labels` adds labels to it.
- `metrics.grafanaDashboard.enabled=true` adds a ConfigMap with a
  [Grafana dashboard](helm/nix-store-csi/dashboards/nix-store-csi.json) for
  Grafana's dashboard sidecar.

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
| `views` | gauge | |
| `free_inodes` | gauge | |
| `gc_duration_seconds` | histogram | `result` |
| `gc_deleted_paths_total` | counter | |
| `gc_freed_bytes_total` | counter | |
| `prefetched` | gauge | `revision` |
| `build_info` | gauge | `version` |

A publish that is still fetching after 90 seconds counts as `Unavailable`, and
kubelet's retry counts again. `nar_fetched_bytes_total`,
`gc_freed_bytes_total` and `store_size_bytes` count uncompressed NAR sizes.
`closures_in_flight` counts closures still being fetched, including those
whose publish already returned `Unavailable`. `free_inodes` is missing on a
filesystem with no limit on inodes, like btrfs.
`prefetched` is 1 once the node store holds the closures of that revision of
the `prefetch` list for the node's system.

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
