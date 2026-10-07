# TODO

## v1

- [ ] Release v0.1.0, after deleting the 0.1.0 images and chart already on
      GHCR, which the release would overwrite.

## After v1

- [ ] v2, on the `v2` branch: views as lazy EROFS mounts, whose files are
      fetched when a pod first reads them.
- [ ] Add a snix castore backend, for chunked files, deduplication across
      store objects and per-blob verification.
