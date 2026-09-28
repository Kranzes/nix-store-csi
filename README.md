# nix-store-csi

nix-store-csi mounts Nix store paths in Kubernetes pods. A pod lists store
paths in an inline volume. The plugin fetches their closure from binary caches
into a store shared by the node, then mounts a view holding only that closure
at the pod's `/nix/store`. Pods run in the runner image, which holds tini and a
mount point for the store.

## Try it

`nix-store-csi unpack` fills a store the way the plugin does, so you can try
it with podman:

```sh
store_path=$(nix eval --raw nixpkgs#pkgs --apply 'pkgs: pkgs.lib.getExe pkgs.hello')
store=$(nix run github:Kranzes/nix-store-csi -- --state-dir /tmp/nix-store-csi unpack "$store_path")
podman load < "$(nix build github:Kranzes/nix-store-csi#runner-image --print-out-paths --no-link)"
podman run --rm -v "$store":/nix/store:ro localhost/nix-store-csi-runner:0.1.0 "$store_path"
```

## Documentation

- [Deploying on Kubernetes](deploy/README.md)
- [Command line](docs/cli.md)
- [Architecture](docs/ARCHITECTURE.md)
