# TODO

## v1

- [ ] Try the chart on a real cluster, starting with EKS on AL2023 nodes.
- [ ] Check SELinux-enforcing nodes like Bottlerocket. A bind mount can't take
      `context=`, so the state dir's files may need a label containers can
      read.
- [ ] Report to Harmonia that its NAR parser accepts nonzero padding and
      trailing data, which Nix rejects. nix-store-csi checks NarSize and
      NarHash before a path enters the store, so it isn't affected.
- [ ] Make Harmonia's `restore` write from one blocking thread, with a cap on
      the file data queued for it. It goes through `tokio::fs`, which hops
      threads for every file operation, so `rust/src/nar.rs` has its own writer
      until then.

## After v1

- [ ] Serve the view over FUSE and fetch on demand, so pods start before
      their whole closure is on the node. The plugin runs as root, so it can
      use FUSE passthrough on Linux 6.9 or later.
- [ ] Add a snix castore backend, for chunked files, dedup across store paths
      and per-blob verification.
