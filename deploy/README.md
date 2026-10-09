# Deploying

## Install

The plugin needs:

- Kubernetes 1.25 or later
- Linux nodes on x86_64 or aarch64
- a namespace that allows privileged pods, like `kube-system`
- `kubeletDir` set to kubelet's root directory, if it isn't `/var/lib/kubelet`
- disk space in `stateDir`, `/var/lib/nix-store-csi` by default, for the
  closures that pods use

Like most CSI drivers, the node plugin goes in `kube-system`.

```sh
helm install nix-store-csi oci://ghcr.io/kranzes/charts/nix-store-csi -n kube-system
```

[values.yaml](helm/nix-store-csi/values.yaml) lists the chart's settings, and
[CLI.md](../docs/CLI.md) lists the plugin flags that they set.

The node plugin runs privileged. In a namespace other than `kube-system` that
enforces a Pod Security level, the level has to be `privileged`. Its volumes
are allowed at the restricted level, so pods that use them pass it with the
usual `securityContext`, as in the example below.

## Example

[hello-pod.yaml](hello-pod.yaml) runs GNU hello on an amd64 or arm64 node:

```sh
kubectl apply -f https://raw.githubusercontent.com/Kranzes/nix-store-csi/master/deploy/hello-pod.yaml
kubectl logs hello
```

A pod stays in `ContainerCreating` until its closure is on the node. Past 90
seconds, its events show how much has arrived.

## Private caches

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

## Injecting store paths into manifests

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

A derivation fills in the variables. Each name in its `storePaths` set becomes
a `NIX_STORE_PATH_` variable:

```nix
{
  lib,
  runCommand,
  gettext,
  hello,
}:
let
  # Each name becomes NIX_STORE_PATH_<name> in the manifests
  storePaths = {
    HELLO = hello;
  };
in
runCommand "manifests"
  {
    __structuredAttrs = true;
    nativeBuildInputs = [ gettext ];
    env = lib.mapAttrs' (
      name: path:
      lib.nameValuePair "NIX_STORE_PATH_${name}" (builtins.unsafeDiscardStringContext "${path}")
    ) storePaths;
  }
  ''
    format=$(printf '$%s ' "''${!NIX_STORE_PATH_@}")
    cd ${./manifests}
    find . -type f | while read -r file; do
      for name in $(envsubst --variables "$(< "$file")"); do
        if [[ $name == NIX_STORE_PATH_* && ! -v $name ]]; then
          echo "$file uses $name, which storePaths doesn't set" >&2
          exit 1
        fi
      done
      mkdir -p "$out/$(dirname "$file")"
      envsubst "$format" < "$file" > "$out/$file"
    done
  ''
```

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

## Multiple architectures

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

## Your own image

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

  Add `TINI = tini` to the derivation's `storePaths` from
  [Injecting store paths into manifests](#injecting-store-paths-into-manifests).
- The image provides what the program expects from its environment, like users
  in `/etc/passwd`, `/tmp` and CA certificates.

Keep store paths out of the image, and the program's store path only in `args`
and `storePaths`. The image then doesn't change when the program does. For
example, this base image adds a company CA to nixpkgs' certificates. It copies
what it needs instead of linking into the store, and runs as nobody, as the
restricted Pod Security level wants:

```nix
dockerTools.buildLayeredImage {
  name = "base";
  extraCommands = ''
    install -m 555 ${pkgsStatic.tini}/bin/tini tini
    mkdir -p nix/store tmp etc/ssl/certs
    chmod 1777 tmp
    install -m 444 ${dockerTools.fakeNss}/etc/{passwd,group,nsswitch.conf} etc/
    cat ${cacert}/etc/ssl/certs/ca-bundle.crt ${./company-ca.pem} > etc/ssl/certs/ca-certificates.crt
  '';
  config = {
    Entrypoint = [ "/tini" "-s" "--" ];
    User = "65534:65534";
  };
}
```

Pods use it like the runner image, with the program in `args` and `storePaths`.

To run an existing image built with `dockerTools.buildLayeredImage`, set
`includeStorePaths = false`. The image then holds only links into `/nix/store`,
and `storePaths` has to list the store paths it links to. Rebuild the image when
those paths change.

## Metrics

With `metrics.enabled=true`, the node plugin serves Prometheus metrics at
`/metrics` on port 9809 (`metrics.port`).

- `metrics.podMonitor.enabled=true` adds a PodMonitor for the Prometheus
  Operator. `metrics.podMonitor.labels` adds labels to it.
- `metrics.grafanaDashboard.enabled=true` adds a ConfigMap with a
  [Grafana dashboard](helm/nix-store-csi/dashboards/nix-store-csi.json) for
  Grafana's dashboard sidecar. The flake also exports the dashboard as
  `dashboards.nix-store-csi`.

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
| `build_info` | gauge | `version` |

A publish that is still fetching after 90 seconds counts as `Unavailable`, and
kubelet's retry counts again. `nar_fetched_bytes_total`,
`gc_freed_bytes_total` and `store_size_bytes` count uncompressed NAR sizes.
`closures_in_flight` counts closures still being fetched, including those
whose publish already returned `Unavailable`. `free_inodes` is missing on a
filesystem with no limit on inodes, like btrfs.

## Security

The CSIDriver is cluster-scoped, and every Pod Security level allows its
volumes. So anyone who can create a pod in any namespace can mount the closure
of any store path that the configured caches serve. That includes private
caches that the plugin reaches with the netrc Secret or a service account
token.

Pods can ask for store paths whose closures are any size. A node fetches the
closure of every pod it runs, and the fetch goes on after the pod is gone.
Collection can't delete store paths that a fetch or a mounted view holds, so
pods with big closures can fill the state dir's disk.

A pod gets only its own closure, on a read-only mount.

To limit who can use the driver, refuse its volumes outside chosen namespaces
with an admission policy, such as a ValidatingAdmissionPolicy that checks
`spec.volumes[].csi.driver`.
