# Command line

```console
$ nix-store-csi --help
Serves Nix store closures from binary caches to Kubernetes pods as CSI volumes

Usage: nix-store-csi [OPTIONS] <COMMAND>

Commands:
  csi     Serve the CSI node plugin on a unix socket
  unpack  Fetch the closures of ROOTs into the node store and print its directory, for use with `podman -v DIR:/nix/store`
  help    Print this message or the help of the given subcommand(s)

Options:
      --substituters <URLS>         Binary caches [env: NIX_STORE_CSI_SUBSTITUTERS=] [default: https://cache.nixos.org]
      --trusted-public-keys <KEYS>  Keys that narinfos must be signed by [env: NIX_STORE_CSI_TRUSTED_PUBLIC_KEYS=] [default: cache.nixos.org-1:6NCHdD59X431o0gWypbMrAURkbJ16ZPMQFGspcDShjY=]
      --no-require-sigs             Accept narinfos without a trusted signature
      --state-dir <DIR>             Holds the node store, in `store/` [env: NIX_STORE_CSI_STATE_DIR=] [default: /var/lib/nix-store-csi]
      --log-level <LEVEL>           off, error, warn, info, debug or trace [env: NIX_STORE_CSI_LOG_LEVEL=] [default: info]
      --log-format <FORMAT>         [env: NIX_STORE_CSI_LOG_FORMAT=] [default: text] [possible values: text, json]
      --max-substitution-jobs <N>   NARs to fetch at once [env: NIX_STORE_CSI_MAX_SUBSTITUTION_JOBS=] [default: 16]
      --store-dir <DIR>             Store dir that store paths are named under [default: /nix/store]
  -h, --help                        Print help
  -V, --version                     Print version
```

## `csi`

```console
$ nix-store-csi csi --help
Serve the CSI node plugin on a unix socket

Usage: nix-store-csi csi [OPTIONS] --endpoint <SOCKET> --node-id <NAME>

Options:
      --endpoint <SOCKET>            Socket path or `unix://` URL [env: CSI_ENDPOINT=]
      --substituters <URLS>          Binary caches [env: NIX_STORE_CSI_SUBSTITUTERS=] [default: https://cache.nixos.org]
      --node-id <NAME>               This node's name, as kubelet knows it [env: NODE_NAME=]
      --trusted-public-keys <KEYS>   Keys that narinfos must be signed by [env: NIX_STORE_CSI_TRUSTED_PUBLIC_KEYS=] [default: cache.nixos.org-1:6NCHdD59X431o0gWypbMrAURkbJ16ZPMQFGspcDShjY=]
      --driver-name <NAME>           Name that pods give as the volume's `driver` [default: nix-store-csi]
      --no-require-sigs              Accept narinfos without a trusted signature
      --gc-after <DURATION>          Delete store paths no pod has used for this long, like `24h` or `7d`. `0s` keeps everything [env: NIX_STORE_CSI_GC_AFTER=] [default: 24h]
      --state-dir <DIR>              Holds the node store, in `store/` [env: NIX_STORE_CSI_STATE_DIR=] [default: /var/lib/nix-store-csi]
      --gc-high-threshold <PERCENT>  Collect when the state dir's filesystem is fuller than this percent, least recently used first. `100` turns this off [env: NIX_STORE_CSI_GC_HIGH_THRESHOLD=] [default: 85]
      --log-level <LEVEL>            off, error, warn, info, debug or trace [env: NIX_STORE_CSI_LOG_LEVEL=] [default: info]
      --gc-low-threshold <PERCENT>   How full to collect down to [env: NIX_STORE_CSI_GC_LOW_THRESHOLD=] [default: 80]
      --log-format <FORMAT>          [env: NIX_STORE_CSI_LOG_FORMAT=] [default: text] [possible values: text, json]
      --max-substitution-jobs <N>    NARs to fetch at once [env: NIX_STORE_CSI_MAX_SUBSTITUTION_JOBS=] [default: 16]
      --store-dir <DIR>              Store dir that store paths are named under [default: /nix/store]
  -h, --help                         Print help
```

## `unpack`

```console
$ nix-store-csi unpack --help
Fetch the closures of ROOTs into the node store and print its directory, for use with `podman -v DIR:/nix/store`

Usage: nix-store-csi unpack [OPTIONS] <ROOT>...

Arguments:
  <ROOT>...  Store paths or their base names

Options:
      --substituters <URLS>         Binary caches [env: NIX_STORE_CSI_SUBSTITUTERS=] [default: https://cache.nixos.org]
      --trusted-public-keys <KEYS>  Keys that narinfos must be signed by [env: NIX_STORE_CSI_TRUSTED_PUBLIC_KEYS=] [default: cache.nixos.org-1:6NCHdD59X431o0gWypbMrAURkbJ16ZPMQFGspcDShjY=]
      --no-require-sigs             Accept narinfos without a trusted signature
      --state-dir <DIR>             Holds the node store, in `store/` [env: NIX_STORE_CSI_STATE_DIR=] [default: /var/lib/nix-store-csi]
      --log-level <LEVEL>           off, error, warn, info, debug or trace [env: NIX_STORE_CSI_LOG_LEVEL=] [default: info]
      --log-format <FORMAT>         [env: NIX_STORE_CSI_LOG_FORMAT=] [default: text] [possible values: text, json]
      --max-substitution-jobs <N>   NARs to fetch at once [env: NIX_STORE_CSI_MAX_SUBSTITUTION_JOBS=] [default: 16]
      --store-dir <DIR>             Store dir that store paths are named under [default: /nix/store]
  -h, --help                        Print help
```
