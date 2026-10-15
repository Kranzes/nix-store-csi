# Changelog

## Unreleased

### Changed

- The chart needs Kubernetes 1.30 or later, up from 1.25, so that
  `admissionPolicy` works on every version it installs on.

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
