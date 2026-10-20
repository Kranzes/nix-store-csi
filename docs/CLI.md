# Command line

```console
$ nix-store-csi --help
Serves Nix store closures from binary caches to Kubernetes pods as CSI volumes

Usage: nix-store-csi [OPTIONS] <COMMAND>

Commands:
  csi     Serve the CSI node plugin on a unix socket
  unpack  Fetch the closures of store paths into the node store and print its directory, for use with `podman -v DIR:/nix/store:ro`
  help    Print this message or the help of the given subcommand(s)

Options:
      --substituters <URLS>         Binary caches [env: NIX_STORE_CSI_SUBSTITUTERS=] [default: https://cache.nixos.org]
      --trusted-public-keys <KEYS>  Keys that narinfos must be signed by [env: NIX_STORE_CSI_TRUSTED_PUBLIC_KEYS=] [default: cache.nixos.org-1:6NCHdD59X431o0gWypbMrAURkbJ16ZPMQFGspcDShjY=]
      --no-require-sigs             Accept narinfos without a trusted signature [env: NIX_STORE_CSI_NO_REQUIRE_SIGS=]
      --netrc-file <FILE>           Netrc file with credentials for caches, read on every request [env: NIX_STORE_CSI_NETRC_FILE=]
      --state-dir <DIR>             Directory that holds the node store in `store/` [env: NIX_STORE_CSI_STATE_DIR=] [default: /var/lib/nix-store-csi]
      --log-level <LEVEL>           off, error, warn, info, debug or trace [env: NIX_STORE_CSI_LOG_LEVEL=] [default: info]
      --log-format <FORMAT>         text or json [env: NIX_STORE_CSI_LOG_FORMAT=] [default: text] [possible values: text, json]
      --max-substitution-jobs <N>   NARs to fetch at once [env: NIX_STORE_CSI_MAX_SUBSTITUTION_JOBS=] [default: 16]
  -h, --help                        Print help
  -V, --version                     Print version
```

## `csi`

```console
$ nix-store-csi csi --help
Serve the CSI node plugin on a unix socket

Usage: nix-store-csi csi [OPTIONS] --endpoint <SOCKET> --node-id <NAME>

Options:
      --endpoint <SOCKET>           Socket path or `unix://` URL [env: CSI_ENDPOINT=]
      --substituters <URLS>         Binary caches [env: NIX_STORE_CSI_SUBSTITUTERS=] [default: https://cache.nixos.org]
      --node-id <NAME>              This node's name, as kubelet knows it [env: NODE_NAME=]
      --trusted-public-keys <KEYS>  Keys that narinfos must be signed by [env: NIX_STORE_CSI_TRUSTED_PUBLIC_KEYS=] [default: cache.nixos.org-1:6NCHdD59X431o0gWypbMrAURkbJ16ZPMQFGspcDShjY=]
      --keep-recent <DURATION>      Keep store paths that a pod used in the last DURATION, like `24h` or `7d`, unless `--ensure-free` needs the space or inodes [env: NIX_STORE_CSI_KEEP_RECENT=] [default: 24h]
      --no-require-sigs             Accept narinfos without a trusted signature [env: NIX_STORE_CSI_NO_REQUIRE_SIGS=]
      --gc-interval <DURATION>      Collect garbage every DURATION, like `10m` or `1h`. While `--ensure-free` needs the space or inodes, collection also runs every minute [env: NIX_STORE_CSI_GC_INTERVAL=] [default: 10m]
      --netrc-file <FILE>           Netrc file with credentials for caches, read on every request [env: NIX_STORE_CSI_NETRC_FILE=]
      --ensure-free <SIZE>          Free space to keep on the state dir's filesystem, like `50G` or `20%`, by deleting unused store paths, least recently used first. It keeps the same share of inodes free, so `50G` of a 500G filesystem keeps 10% of its inodes free. `0` turns this off [env: NIX_STORE_CSI_ENSURE_FREE=] [default: 20%]
      --state-dir <DIR>             Directory that holds the node store in `store/` [env: NIX_STORE_CSI_STATE_DIR=] [default: /var/lib/nix-store-csi]
      --log-level <LEVEL>           off, error, warn, info, debug or trace [env: NIX_STORE_CSI_LOG_LEVEL=] [default: info]
      --metrics-address <ADDRESS>   Serve Prometheus metrics at `/metrics` on ADDRESS, like `[::]:9809`. Off unless set [env: NIX_STORE_CSI_METRICS_ADDRESS=]
      --log-format <FORMAT>         text or json [env: NIX_STORE_CSI_LOG_FORMAT=] [default: text] [possible values: text, json]
      --max-closure-size <SIZE>     Refuse a volume whose closure's NARs add up to more than SIZE, like `20G`, before fetching any of it. Prefetched closures don't count. Off unless set [env: NIX_STORE_CSI_MAX_CLOSURE_SIZE=]
      --max-substitution-jobs <N>   NARs to fetch at once [env: NIX_STORE_CSI_MAX_SUBSTITUTION_JOBS=] [default: 16]
      --prefetch-dir <DIR>          Keep the closures of the store paths listed in DIR fetched, even when `--ensure-free` needs the space. DIR, such as a mounted `ConfigMap`, holds a file for each volume attribute, `storePaths` or `storePaths.<system>`, and may hold a `revision` file, which the `prefetched` metric reports. The plugin reads it when it changes and every minute. Off unless set [env: NIX_STORE_CSI_PREFETCH_DIR=]
  -h, --help                        Print help
```

## `unpack`

```console
$ nix-store-csi unpack --help
Fetch the closures of store paths into the node store and print its directory, for use with `podman -v DIR:/nix/store:ro`

Usage: nix-store-csi unpack [OPTIONS] <STORE_PATH>...

Arguments:
  <STORE_PATH>...  Store paths, their base names, or paths inside them

Options:
      --dry-run                     Only look up the closures in the caches and print their size, or fail if a store path is missing. This fetches no NARs and doesn't use the state dir
      --substituters <URLS>         Binary caches [env: NIX_STORE_CSI_SUBSTITUTERS=] [default: https://cache.nixos.org]
      --trusted-public-keys <KEYS>  Keys that narinfos must be signed by [env: NIX_STORE_CSI_TRUSTED_PUBLIC_KEYS=] [default: cache.nixos.org-1:6NCHdD59X431o0gWypbMrAURkbJ16ZPMQFGspcDShjY=]
      --no-require-sigs             Accept narinfos without a trusted signature [env: NIX_STORE_CSI_NO_REQUIRE_SIGS=]
      --netrc-file <FILE>           Netrc file with credentials for caches, read on every request [env: NIX_STORE_CSI_NETRC_FILE=]
      --state-dir <DIR>             Directory that holds the node store in `store/` [env: NIX_STORE_CSI_STATE_DIR=] [default: /var/lib/nix-store-csi]
      --log-level <LEVEL>           off, error, warn, info, debug or trace [env: NIX_STORE_CSI_LOG_LEVEL=] [default: info]
      --log-format <FORMAT>         text or json [env: NIX_STORE_CSI_LOG_FORMAT=] [default: text] [possible values: text, json]
      --max-substitution-jobs <N>   NARs to fetch at once [env: NIX_STORE_CSI_MAX_SUBSTITUTION_JOBS=] [default: 16]
  -h, --help                        Print help
```
