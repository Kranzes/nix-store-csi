# nix-store-csi

nix-store-csi runs Nix store paths in Kubernetes pods without building an image
for them. It is a CSI node plugin. A pod lists store paths in an inline
volume. The plugin fetches their closure from binary caches into a store
shared by the node, then mounts a view holding only that closure at the pod's
`/nix/store`. Pods run in the runner image, which holds tini and a mount point
for the store.

## Try it

`nix-store-csi unpack` fills a store the way the plugin does, so you can try
it with podman:

```sh
hello=$(nix eval --raw nixpkgs#hello.outPath)
store=$(nix run . -- --state-dir ./state unpack "$hello")
podman load < "$(nix build .#runner-image --print-out-paths --no-link)"
podman run --rm -v "$store":/nix/store:ro localhost/nix-store-csi-runner:0.1.0 "$hello/bin/hello"
```

To run it on Kubernetes, see [deploy/](deploy/README.md).

## Usage

```
nix-store-csi [OPTIONS] csi --endpoint <SOCKET> --node-id <NAME>
nix-store-csi [OPTIONS] unpack <ROOT>...
```

`csi` serves the node plugin. Its `--gc-after <DURATION>`
(`NIX_STORE_CSI_GC_AFTER`, default `24h`) deletes store paths no pod has used
for that long. `unpack` fetches the closures of the roots into
`<state-dir>/store` and prints that directory.

| Option | Env | Default |
|---|---|---|
| `--substituters <URLS>`, `--extra-substituters` | `NIX_STORE_CSI_SUBSTITUTERS`, `NIX_STORE_CSI_EXTRA_SUBSTITUTERS` | `https://cache.nixos.org` |
| `--trusted-public-keys <KEYS>`, `--extra-trusted-public-keys` | `NIX_STORE_CSI_TRUSTED_PUBLIC_KEYS`, `NIX_STORE_CSI_EXTRA_TRUSTED_PUBLIC_KEYS` | `cache.nixos.org-1:6NCHdD59X431o0gWypbMrAURkbJ16ZPMQFGspcDShjY=` |
| `--no-require-sigs` | | off |
| `--state-dir <DIR>` | `NIX_STORE_CSI_STATE_DIR` | `/var/lib/nix-store-csi` |
| `--max-substitution-jobs <N>` | `NIX_STORE_CSI_MAX_SUBSTITUTION_JOBS` | 16 |
| `--log-level <LEVEL>` | `NIX_STORE_CSI_LOG_LEVEL` | `info` |

Lists are space-separated, as in Nix. A later `--substituters` or
`--trusted-public-keys` replaces an earlier one. nix-store-csi tries caches in
priority order, like Nix.

`nix-store-csi --help` lists every option.
[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) explains how it works.

## Building and testing

- `nix build` builds nix-store-csi, `nix build .#csi-image` the node plugin's
  image, and `nix build .#runner-image` the image pods run.
- Building needs protoc and `CSI_PROTO`, the CSI spec's `csi.proto`.
  `cargo test` also needs `NIX_STORE_CSI_FIXTURES`, the test caches that
  `nix/fixtures.nix` builds. `nix develop` provides all three.
  `NIX_STORE_CSI_NETWORK_TESTS=1` adds tests against cache.nixos.org.
- `nix flake check` runs clippy, the tests, cargo-deny, cargo-audit,
  `helm lint`, treefmt, and a NixOS VM test that installs the chart on
  one-node k3s.

## License

MIT, see [LICENSE](LICENSE).
