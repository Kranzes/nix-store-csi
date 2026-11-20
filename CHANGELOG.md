# Changelog

## Unreleased

### Changed

- A publish links the subdirectories of each store path into the view on
  several threads, and uses at least 16 threads on nodes with fewer CPUs.
  With cold caches every directory waits for a disk read, so on one thread
  a store path with tens of thousands of files, like a Python environment,
  took seconds to link.

## 0.1.20 - 2026-11-20

### Added

- The `published` line says where a publish's time went: `fetch_ms` until the
  closure was in the node store, counted from kubelet's first try, `wait_ms`
  for the volume's turn and for garbage collection, `view_ms` to build the
  view, `record_ms` to put it in place and write the volume's record, with
  the wait for other volumes that do the same, and `mount_ms`. They add up to
  `elapsed_ms`.

### Changed

- The node plugin's pod starts the registrar and the liveness probe before
  the plugin. Kubelet pulls a pod's images in the order of its containers, so
  on a new node the prefetch wrote to the disk while kubelet pulled the other
  two images, and the node's first mount waited for the registrar's.
  `kubectl logs` and `kubectl exec` still pick the plugin's container.

### Fixed

- A publish no longer syncs the volume's record to disk. On a disk that was
  still writing the data of a fetch, the sync took seconds, and the publishes
  of other volumes waited for it. Only a power loss can lose the record, and
  it unmounts the volume too.

## 0.1.19 - 2026-11-20

### Changed

- The dashboard's Prefetch rollout panel shows the node plugins still
  fetching each prefetch list, so only lists that are rolling out show up.
  The Prefetch lists table is wider and gives each hash of a list its system,
  its pool with node pools, and the share of node plugins that fetched it.
  The prefetch alerts point to the table.
- While a publish's fetches are in flight, the plugin starts no new fetch for
  the prefetch lists. The NARs that it is already fetching for them finish.
  Before, the publish took turns with each list, so a pod that started during
  a large prefetch waited for most of it.

### Fixed

- A fetch can no longer bring a store path back between the moment that
  collection deletes it and the moment that collection deletes its narinfo.
  Such a path lost its new narinfo, and stayed in the node store without one
  until the next restart. The same holds for the deletion after a failed
  sync.
- A start-up whose sync of the node store fails leaves the record of a failed
  sync. If the plugin exited before it had deleted the store paths that the
  last run left unsynced, the next start-up kept them as durable, since the
  kernel reports a lost write only once.
- A publish fails if a failed sync deleted store paths after its closure was
  resolved, and kubelet's next try resolves the closure again. With caches
  that disagree on a store path's references, such a publish could have
  mounted a closure that lacks one of them.

## 0.1.18 - 2026-11-19

### Added

- At debug level, `chose how to fetch the NAR` says whether each NAR comes in
  ranges or in one stream, and why, and `fetched the NAR in ranges` how many
  ranges it took and how long it waited for the cap on them. `fetching NAR`
  and `fetched NAR` give the narinfo's `FileSize`.

### Changed

- From S3 and GCS, an uncompressed NAR comes in ranges too, since object
  stores send files as stored.
- Ranges are 8 MiB instead of 4 MiB, since each waits for the cache's first
  byte, which S3 can take 100 ms to send. Ranges that arrive before
  decompression needs them take at most 512 MiB instead of 256 MiB, and a NAR
  whose narinfo gives a `FileSize` of 8 MiB or less comes in one request.

### Fixed

- A compressed NAR whose narinfo has no `FileSize` comes in ranges too, with
  its size from the first range. 0.1.17 fetched it in one request. Without a
  `FileSize`, even a small NAR's first request counts towards the 64 ranges
  in flight.

## 0.1.17 - 2026-11-19

### Added

- Each fetched NAR logs `fetched NAR` at debug level, with the fields of
  `fetching NAR` and `elapsed_ms`.

### Changed

- A closure's largest NARs get fetch slots first instead of waiting behind
  smaller ones.
- From a cache that answers range requests over HTTP/1.1, like S3, a
  compressed NAR over 4 MiB comes in 4 MiB ranges over up to 16 connections,
  so a cache that limits each connection no longer caps it at one
  connection's speed. At most 64 ranges are in flight on a node, so ranges that
  arrive before decompression needs them take at most 256 MiB. The NAR still
  counts once towards `--max-substitution-jobs`.
- The plugin unpacks NARs on up to three threads per job instead of one,
  still at most one per CPU, so a NAR's decompression, hashing and parsing
  each get a thread.
- `--max-substitution-jobs` defaults to twice the CPUs, from 8 to 64, instead
  of 16, so nodes with more CPUs fetch closures of many small NARs faster.
- A large fetch's peak memory now scales with the node's CPUs, which set the
  jobs and unpack threads. Measured on closures of 13.7 and 6.3 GB:

  | CPUs (jobs) | Peak RSS       |
  | ----------- | -------------- |
  | 4 (8)       | 260 to 370 MiB |
  | 16 (32)     | 790 to 940 MiB |
  | 32 (64)     | 1.0 to 1.3 GiB |
  | 88 (64)     | 1.3 to 1.7 GiB |

  A request is not a cap, and the chart sets no memory limit.
- The chart's memory request rises from 256Mi to 512Mi, so on a node without
  that much unreserved memory the plugin's pod stays Pending after the
  upgrade. Mounted volumes keep working, but no new volume can be published
  there until the pod fits or the request is lowered.

## 0.1.16 - 2026-11-19

### Added

- `nodePools.<pool>.stateDir` replaces `stateDir` on a pool's nodes, for nodes
  whose bigger disk is mounted elsewhere, like an instance store.
- When volumes on a node are mounted from views in another state dir, like
  after a change of `stateDir`, the plugin logs a warning at start with that
  dir, its device and the number of volumes.
  `nix_store_csi_other_state_dir_volumes` counts them. Kubelet unpublishes
  them as usual. The plugin adds no mountpoints to their views and never
  deletes the old dir.

### Changed

- A `stateDir` that is a symlink to a dir outside its parent no longer works.
  The chart now mounts the state dir as a subPath of its parent, and kubelet
  refuses a subPath that resolves outside the volume. The plugin's pod then
  fails with `failed to prepare subPath for volumeMount "state"`. Set
  `stateDir` to the link's target. Since every DaemonSet renders differently,
  the upgrade restarts every plugin pod, as a new image does anyway.
- Neither the plugin nor the chart creates the state dir's parent any more.
  The plugin's pod stays in `ContainerCreating` until the parent exists, and
  its events name it. The chart refuses a `stateDir` in `kubeletDir`.

## 0.1.15 - 2026-11-19

### Added

- `mountpoints.cdi` and `mountpoints.paths`, or `--host-root` and
  `--mountpoints`, give each volume empty mountpoints where the container
  runtime binds under `/nix/store`, from the node's CDI specs or a list.
  Without them, such a container fails with `read-only file system`, like a
  GPU pod on NixOS. The plugin reads the specs every 10 seconds and adds new
  mountpoints to mounted volumes, so restarted containers find them after a
  rebuild.

## 0.1.14 - 2026-11-18

### Fixed

- The images and the chart are published again. 0.1.13 has neither, because
  building the images' SBOMs failed on a wrong source hash for
  cargo-cyclonedx in nixpkgs. 0.1.14 is 0.1.13 with that fixed.

## 0.1.13 - 2026-11-18

### Added

- `nix_store_csi_locality_best_score`, a histogram of the best score in each
  of the locality extender's answers, by scheduler. Mostly 0 means no
  candidate node held the pod's closure. High while publishes still fetch
  means a candidate held it and the scheduler chose another node.
- `nix_store_csi_prefetched_size_bytes`, the NAR size of the closure that
  each prefetch list holds, with the same labels as
  `nix_store_csi_prefetched`. A store path in several lists counts in each.
- The dashboard has a Prefetch list size panel, and a Locality row, collapsed,
  for the locality extender's answers, scrapes and closure resolves.
- `nodePools.<pool>.inheritPrefetch` makes a pool's nodes read the top-level
  prefetch lists, from `prefetch` or `prefetchConfigMap`, beside the pool's,
  so a closure that every node needs goes in one list. The chart refuses a key
  that both `prefetch` and the pool's `prefetch` set, and a `storePaths` in
  one that the other's key for a system would hide.
- The `fetched the closure` log line has a `caches` field, a JSON object with
  the NAR bytes that each cache served for the closure, as
  `nar_fetched_bytes_total` counts them.
- `nix_store_csi_nar_received_bytes_total`, the uncompressed NAR bytes
  received from each cache as they arrive, also from tries that failed or
  started over. The dashboard's NAR throughput panel plots it.

### Changed

- Once a node holds a prefetch list's new version, or the list empties or its
  key goes, the old closure counts as just used. Collection keeps it for
  `--keep-recent` unless `--ensure-free` needs the space. Before, it counted as
  last used at the node's last ensure of the old version. A new version that
  took longer than `--keep-recent` to fetch, or kept failing, left the old
  closure to the next collection.

### Fixed

- The dashboard's Prefetch lists table shows hashes and list names as text. A
  hash like `241e89981382` showed as ∞.
- The dashboard's counts skip the staleness markers that a failed scrape
  writes. Before, the interval after one showed everything that the plugin
  had counted since it started. The stats also count across a restart of the
  plugin's container.
- The dashboard's Publish time and NAR fetch time panels ask for at most 200
  points, so their queries stay under Prometheus's default limit of 50 million
  samples on clusters of about 1,000 nodes.

## 0.1.12 - 2026-11-17

### Added

- `--prefetch-dir` and the chart's `prefetch` value also take lists named
  `storePaths.<system>.<name>`, so values files or other owners of the
  ConfigMap can each keep their own. A node prefetches every list for its
  system, or if there is none, `storePaths`. It fetches each list on its own,
  so one that fails doesn't stop the others.
- `nix_store_csi_build_info` has a `system` label, the node's Nix system.
- The chart's `prefetchConfigMap`, at the top level and in each node pool,
  names a ConfigMap to mount as the prefetch dir in place of one that the
  chart renders from `prefetch`, so owners can write their keys outside Helm.
  The plugin starts before the ConfigMap exists and reads its lists once it
  does.
- The chart's `imagePullSecrets` sets the pods' pull Secrets.
- `--max-store-size` and the chart's `maxStoreSize` also take a share of the
  state dir's filesystem, like `50%`.
- The chart's `nodePools.<pool>.maxStoreSize` replaces the top-level
  `maxStoreSize` on a pool's nodes.
- `locality`, and the chart's `locality.enabled`, run a scheduler extender
  for kube-scheduler and Volcano. It scores each node by the share of a pod's
  closure, by NAR size, that the node store holds. `locality.nodeSelector`
  and `locality.tolerations` place its pod. The `NixStoreCsiLocalityDown`,
  `NixStoreCsiLocalityScrapeErrors` and `NixStoreCsiLocalityResolveErrors`
  alerts cover it.
- With `--serve-paths`, the plugin serves `/store-paths`, the node store's
  store paths, which `locality` reads. Its `ETag` changes when the node store
  gains or loses a store path, and a request with the current one gets 304.

### Changed

- Breaking: `nix_store_csi_prefetched` drops its `revision` label. It has a
  series for each prefetch list, with `list` and `hash` labels, so an owner
  can wait for its own list. The plugin ignores a `revision` file in
  `--prefetch-dir`, and the chart's prefetch ConfigMaps lose their `revision`
  key. Queries, dashboards and alerts outside the chart that select `revision`
  need the new labels, and the chart's notes print the new rollout query.
  `NixStoreCsiPrefetchBehind` fires for a list that a node has read but
  doesn't hold, and for a list that the chart renders while a node hasn't read
  its current version.
- The plugin resolves cache host names with hickory-dns instead of musl's
  resolver. It reads `/etc/hosts` and `/etc/resolv.conf` as before.

### Removed

- `/paths`, which listed each store path in the node store with its NAR size,
  last use and why garbage collection keeps it. `--serve-paths` turns on
  `/store-paths` instead.

## 0.1.11 - 2026-11-13

### Added

- `nix_store_csi_publish_duration_seconds` has a `fetched` label, `true` for a
  publish that waited for a fetch or retried after `Unavailable`, and `false`
  for one whose closure the node store already had. It separates the time the
  plugin takes to mount a closure from the time caches take to serve it. The
  dashboard's Publish time panel plots the two apart, on a log scale.

### Fixed

- The chart accepts `s3://` substituters. Its values schema allowed only
  `http(s)://` and `file://`, so Helm refused the install.

## 0.1.10 - 2026-11-12

### Added

- With `--serve-paths`, the plugin serves `/paths` on `--metrics-address`, a
  JSON line for each store path in the node store with its NAR size, last use
  and why garbage collection keeps it. It answers 503 while a listing is
  already running.
- `--max-store-size` and the chart's `maxStoreSize` refuse a volume that needs
  a fetch if the NARs of its closure and those that garbage collection can't
  delete add up to more than the limit. The `nix_store_csi_uncollectable_bytes`
  metric shows what garbage collection can't delete, and
  `nix_store_csi_max_store_size_bytes` shows the limit. The dashboard's Store
  size panel plots the uncollectable size, and the `NixStoreCsiStoreNearLimit`
  alert fires at 90% of the limit. The plugin warns when the NARs of the
  prefetch list's closure add up to more than the limit.
- The chart's `serviceAccount.annotations` sets annotations on the plugin's
  ServiceAccount, like `eks.amazonaws.com/role-arn` for IRSA.

## 0.1.9 - 2026-11-05

### Changed

- A volume's fetches no longer wait behind all the fetches of a closure that
  came first, like the prefetch list on a node that just started. Publishes
  and prefetching take turns at the `--max-substitution-jobs` slots.
- A publish no longer waits while the plugin deletes views or store objects
  in the background, or while garbage collection counts the node store's
  inodes.
- After a failed read of a cache's `nix-cache-info`, the plugin tries again
  after a second instead of a minute, and doubles the wait after each failure.
- After a request to a cache fails, the plugin skips only that file for a
  minute, not the whole cache, unless a second file fails or the cache can't
  be reached.

### Fixed

- After a failed sync of the node store, a publish no longer links store
  objects that the plugin was fetching during that sync, which may have lost
  data. It fails until the plugin fetches them again.
- A NAR URL that leaves a GCS cache's bucket, like `./https:host/x`, fails
  the request. The plugin used to send its GCP token to that host.
- Decoding an xz NAR uses at most 256 MiB. A cache could make each fetch
  allocate up to 4 GiB before the NarHash check failed.

## 0.1.8 - 2026-10-23

### Changed

- Resolving a closure looks up 128 narinfos in parallel instead of 32. Each
  cache gets at most 128 lookups at a time from a node, however many closures
  it resolves, so a cache that speaks only HTTP/1.1 gets at most 128
  connections for narinfos.

### Fixed

- The plugin starts again after it was killed while writing a volume's
  record. The empty temporary file that the kill left in `volumes/` used to
  fail every start and every garbage collection.
- Pod events for a failed fetch or lookup name store paths in full, and name
  a failed NAR's URL once instead of twice.
- A failed TLS handshake with a cache, as for an expired certificate, fails
  the request at once instead of retrying for a minute.

## 0.1.7 - 2026-10-21

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
- `unpack` logs progress every 5 seconds.
- Requests fail at once on 501, 505 and 511 responses instead of retrying
  for a minute, as in Nix.
- After a 429 response or an S3 `SlowDown` error without `Retry-After`, a
  request waits at least 5 seconds before it retries.
- Error messages quote a cache's error response. For S3 and GCS they give
  the error's code and message, like `SignatureDoesNotMatch`. For a plain
  text body they give its first 200 characters.
- Start-up syncs the state dir's filesystem only when the last run left
  store paths unsynced, and otherwise the first fetch does. So `unpack` of a
  closure that the state dir already holds no longer waits for other
  programs' writes to that filesystem.

### Fixed

- A 403 from S3 or GCS counts as a missing file only with the code
  `AccessDenied`. Others, like `SignatureDoesNotMatch` or
  `RequestTimeTooSkewed`, are errors, so wrong credentials no longer look
  like cache misses.
- With `AWS_SKIP_SIGNATURE=true`, `s3://` caches read public buckets without
  credentials. Before, the plugin looked for credentials anyway and failed.

### Added

- Requests to HTTP and S3 caches accept zstd, brotli and gzip responses,
  except for NARs that are compressed already. harmonia-cache, which stores
  NARs uncompressed, then sends them compressed with zstd.
- `unpack --dry-run` checks that the caches have the closures and prints
  their size, without fetching NARs or using the state dir.
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
