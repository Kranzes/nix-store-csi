# nix-store-csi

nix-store-csi mounts Nix store paths in Kubernetes pods. A pod lists store
paths in an inline volume. The plugin fetches their closure from binary caches
into a store shared by the node, then mounts a view holding only that closure
at the pod's `/nix/store`. Pods can run in the runner image, which holds tini,
or in any other image.

## Try it

`nix-store-csi unpack` fills a store the way the plugin does, so you can try
it with podman:

```sh
exe=$(nix eval --raw nixpkgs#pkgs --apply 'pkgs: pkgs.lib.getExe pkgs.hello')
store=$(nix run github:Kranzes/nix-store-csi/v0.1.4 -- --state-dir /tmp/nix-store-csi unpack "$exe")
podman run --rm -v "$store":/nix/store:ro ghcr.io/kranzes/nix-store-csi-runner:0.1.4 "$exe"
```

## Why a CSI driver

[nix-snapshotter](https://github.com/pdtpartners/nix-snapshotter) also runs
Nix store paths in Kubernetes, as a containerd snapshotter that mounts them as
image layers. nix-store-csi mounts them as a volume instead:

- **No images or registry.** A new version is a new store path in the pod
  spec, and pods run in a fixed image like the runner image. nix-snapshotter
  needs a registry, or kubelet pointed at its image service and Nix on the
  node to resolve images by store path.
- **Its own storage model.** Lazy and chunked snapshotters exist, like
  stargz and Nydus, but they need their own image formats served from a
  registry. Here the plugin picks the store's format alone, so it could later
  be lazy via FUSE, or a castore with FastCDC chunks in place of NARs.
- **Nothing to install on nodes.** The plugin is a DaemonSet that fetches NARs
  itself, so nodes need no containerd plugin and no Nix. That suits managed
  node pools, as long as their kernel meets the
  [requirements](deploy/README.md#install). nix-snapshotter runs on the host
  next to containerd and uses the host's Nix store.
- **Any CRI runtime.** Kubelet mounts the volume, so the plugin doesn't depend
  on the runtime. nix-snapshotter needs containerd.

### Trade-offs

- Image scanners, signature checks and registry allowlists don't see
  `storePaths`. Scan an SBOM of the closure instead, e.g. from
  [bombon](https://github.com/nikstur/bombon). The plugin fetches only from
  `substituters` and checks narinfo signatures against `trustedPublicKeys`.
- Any pod that may use the volumes can make a node fetch whatever the trusted
  caches serve. `maxClosureSize` and `admissionPolicy.allowedNamespaces` can
  limit that, see [Security](deploy/README.md#security).

## Documentation

- [Deploying on Kubernetes](deploy/README.md)
- [Command line](docs/CLI.md)
- [Architecture](docs/ARCHITECTURE.md)
- [Changelog](CHANGELOG.md)
