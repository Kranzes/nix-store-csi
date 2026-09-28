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
store=$(nix run github:Kranzes/nix-store-csi -- --state-dir /tmp/nix-store-csi unpack "$exe")
podman run --rm -v "$store":/nix/store:ro ghcr.io/kranzes/nix-store-csi-runner:master "$exe"
```

## Documentation

- [Deploying on Kubernetes](deploy/README.md)
- [Command line](docs/CLI.md)
- [Architecture](docs/ARCHITECTURE.md)
