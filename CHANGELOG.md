# Changelog

## Unreleased

### Changed

- Pod events show the closure's progress, like `still fetching the closure:
  45/210 store paths (1.2/3.4 GiB), 38.1 MiB/s`.
- When no cache has a store path, the error fits on one line and names the
  caches that have it with an untrusted or invalid signature.
- When a volume has no `storePaths` attribute for the node's system, the
  error lists the ones it has.
- Logs carry spans: `publish` and `unpublish` with the volume, `fetch` with
  the store path, and `prefetch`. The fetch's `size` field is now
  `nar_size`, the counts of synced and deleted paths are `synced`, `deleted`
  and `deleting`, and a narinfo without a trusted signature logs at debug
  level.
- Fetches log one line per closure instead of one per NAR.
- Requests fail at once on 501, 505 and 511 responses instead of retrying
  for a minute, as in Nix.
- After a 429 response or an S3 `SlowDown` error without `Retry-After`, a
  request waits at least 5 seconds before it retries.
- Error messages quote a cache's error response. For S3 and GCS they give
  the error's code and message, like `SignatureDoesNotMatch`. For a plain
  text body they give its first 200 characters.

### Fixed

- A 403 from S3 or GCS counts as a missing file only with the code
  `AccessDenied`. Others, like `SignatureDoesNotMatch` or
  `RequestTimeTooSkewed`, are errors, so wrong credentials no longer look
  like cache misses.
- With `AWS_SKIP_SIGNATURE=true`, `s3://` caches read public buckets without
  credentials. Before, the plugin looked for credentials anyway and failed.

### Added

- The `published` and `prefetched` logs have an `elapsed_ms` field.
- `--gc-interval` sets how often collection runs, 10 minutes by default.

## 0.1.6 - 2026-10-16

### Fixed

- The images' SBOMs list the libraries linked into their static binaries:
  musl in both images, and xz and zstd in the CSI image.

## 0.1.5 - 2026-10-16

### Changed

- When a cache answers a request with `Retry-After`, the plugin waits at least
  that long before it retries. If the wait would run past the minute that a
  request retries for, the request fails at once.

### Added

- `s3://` substituters, with the `region`, `endpoint` and `addressing-style`
  settings of Nix's S3 store. `endpoint` takes a URL, like
  `http://localhost:9000`. The plugin presigns each request with credentials
  from the environment: AWS keys, IRSA, EKS Pod Identity or the instance role.
  With `endpoint=https://storage.googleapis.com` and no AWS keys, it reads
  GCS with a token from GKE Workload Identity.
- The chart's `s3CredentialsSecret` value, a Secret whose keys the plugin gets
  as environment variables, for `s3://` caches that take keys, like R2.
- CycloneDX SBOMs for the images, built with bombon, as the `sbom` attribute
  of `csi-image` and `runner-image`. The CSI image's SBOM lists the crates in
  the binary. CI attests each platform's image with its SBOM.

## 0.1.4 - 2026-10-15

### Changed

- The chart needs Kubernetes 1.30 or later, up from 1.25, so that
  `admissionPolicy` works on every version it installs on.
- The plugin unpacks NARs on threads that Landlock confines to the state dir's
  `tmp/`, so a bug in unpacking can't write elsewhere. It needs Linux 6.2 or
  later with `landlock` in the kernel's `lsm=` list, and fails at start-up
  without it.
- The runner image gives nobody a home it can write to, `/home/nobody`, in
  place of `/var/empty`, and sets `SSL_CERT_FILE` and `NIX_SSL_CERT_FILE` to
  its CA certificates.

### Added

- The chart's `nodePools` value gives groups of nodes their own prefetch
  lists, each with its own DaemonSet and ConfigMap. A pool's list replaces
  `prefetch` on its nodes. The PodMonitor adds a `pool` label to the metrics.
- The `nix_store_csi_free_bytes`, `nix_store_csi_ensure_free_bytes` and
  `nix_store_csi_ensure_free_inodes` gauges, which compare the free space and
  inodes on the node store's filesystem with `--ensure-free`, and the
  `nix_store_csi_failed_syncs_total` counter. The dashboard shows the free
  space over `--ensure-free` and the failed syncs.
- The chart's `metrics.prometheusRule` value adds a PrometheusRule with
  alerts for a plugin that Prometheus can't scrape, failing publishes, caches,
  fetches and syncs, a stuck sync, failing collections, too little free space,
  and nodes behind on their prefetch list.

## 0.1.3 - 2026-10-14

### Added

- The `nix_store_csi_prefetched{revision}` metric, a `revision` key in the
  chart's prefetch ConfigMap, and the Prefetch rollout and Prefetch list
  dashboard panels, so a rollout can wait until every node holds the new
  prefetch list.
- `deploy/manifests.nix` writes the chart's `prefetch` value to its `prefetch`
  output.

### Changed

- Prefetched closures don't count towards `--max-closure-size`, and prefetch
  itself has no limit.

## 0.1.2 - 2026-10-12

### Added

- `--prefetch-dir` and the chart's `prefetch` value keep closures fetched on
  every node.
- `--max-closure-size` and the chart's `maxClosureSize` refuse volumes whose
  closures are bigger.
- The plugin answers `NodeGetVolumeStats`, so kubelet reports each volume's
  size.
- `admissionPolicy.allowedNamespaces` limits the namespaces whose pods may use
  the volumes.
- `updateStrategy.rollingUpdate.maxUnavailable` sets how many node plugins the
  chart updates at a time.

### Changed

- The chart refuses to install without trusted public keys.

## 0.1.1 - 2026-10-09

### Added

- `storePaths.<system>` volume attributes and `/nix/store/.roots/<name>` links,
  so one manifest serves several systems.

## 0.1.0 - 2026-10-08

First release.
