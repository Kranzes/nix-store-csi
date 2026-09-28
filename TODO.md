# TODO

## v1

- [ ] Try the chart on a real cluster, starting with EKS on AL2023 nodes.
- [ ] Check SELinux-enforcing nodes like Bottlerocket. A bind mount can't take
      `context=`, so the state dir's files may need a label containers can
      read.
- [ ] Run `nix flake check` in CI. GitHub's Linux runners have KVM for the VM
      checks.
- [ ] Report to Harmonia that its NAR parser accepts nonzero padding and
      trailing data, which Nix rejects. nix-store-csi checks NarSize and
      NarHash before unpacking, so it isn't affected.

## After v1

- [ ] Serve the view over FUSE and fetch on demand, so pods start before
      their whole closure is on the node. The plugin runs as root, so it can
      use FUSE passthrough on Linux 6.9 or later.
- [ ] Add a snix castore backend, for chunked files, dedup across store paths
      and per-blob verification.
