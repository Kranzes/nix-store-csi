# TODO

## v1

- [ ] Drop the `[patch]` section from `rust/Cargo.toml`, and
      `Kranzes/harmonia` from `rust/deny.toml`, once these Harmonia PRs are
      merged:
      - [nix-community/harmonia#1218](https://github.com/nix-community/harmonia/pull/1218),
        `restore` through one blocking writer thread per NAR
      - [nix-community/harmonia#1219](https://github.com/nix-community/harmonia/pull/1219),
        hashing with aws-lc-rs, twice as fast on CPUs without SHA extensions
      - [nix-community/harmonia#1220](https://github.com/nix-community/harmonia/pull/1220),
        rejecting non-zero NAR padding like Nix
      - [nix-community/harmonia#1221](https://github.com/nix-community/harmonia/pull/1221),
        a `CacheInfo` type for `nix-cache-info`
      - [nix-community/harmonia#1227](https://github.com/nix-community/harmonia/pull/1227),
        narinfo hashes in any encoding, which Cachix caches need
- [ ] Release v0.1.0, after deleting the 0.1.0 images and chart already on
      GHCR, which the release would overwrite.

## After v1

- [ ] v2, on the `v2` branch: views as lazy EROFS mounts, whose files are
      fetched when a pod first reads them.
- [ ] Add a snix castore backend, for chunked files, deduplication across
      store objects and per-blob verification.
