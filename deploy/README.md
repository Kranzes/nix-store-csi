# Deploying

`helm/nix-store-csi/` is a Helm chart for the CSI driver and the node plugin,
a privileged DaemonSet with the `node-driver-registrar` sidecar.
`hello-pod.yaml` is a pod that runs GNU hello from cache.nixos.org.

Only the node plugin is privileged. Pods that use its volumes pass the
restricted Pod Security level. The plugin keeps one store per node under
`/var/lib/nix-store-csi`, so a store path downloads once per node and pods
share its page cache.

## Install

The plugin's namespace has to allow privileged pods:

```sh
kubectl create namespace nix-store-csi-system
kubectl label namespace nix-store-csi-system pod-security.kubernetes.io/enforce=privileged
helm install nix-store-csi oci://ghcr.io/kranzes/charts/nix-store-csi -n nix-store-csi-system
kubectl apply -f deploy/hello-pod.yaml
kubectl logs hello
```

A pod stays in `ContainerCreating` until its closure is on the node. If the
fetch takes over 90 seconds, the plugin tells kubelet to retry and keeps
fetching.

## Values

| Value | Default |
|---|---|
| `substituters` | `https://cache.nixos.org` |
| `trustedPublicKeys` | `cache.nixos.org-1:6NCHdD59X431o0gWypbMrAURkbJ16ZPMQFGspcDShjY=` |
| `extraArgs` | none. Takes any other option, like `--gc-after=7d` |
| `image.repository`, `image.tag` | `ghcr.io/kranzes/nix-store-csi`, the chart's `appVersion` |
| `kubeletDir` | `/var/lib/kubelet` |
| `stateDir` | `/var/lib/nix-store-csi` |
| `hostNetwork`, `nodeSelector`, `tolerations`, `resources` | as for any DaemonSet |

The caches and keys apply to every pod on the node. Pods can't choose their
own, because they share what any one of them fetches.

## Images

The images are for amd64 and arm64. Each `v*` tag publishes the chart and both
images to GHCR under its version.
Each push to `master` publishes the images as `master` and the chart as
`<version>-master`, whose images are the `master` ones.

To use your own registry, push the images and set `image.repository`, and the
`image:` in your pods:

```sh
skopeo copy docker-archive:"$(nix build .#csi-image --print-out-paths --no-link)" \
  docker://registry.example.com/nix-store-csi:0.1.0
skopeo copy docker-archive:"$(nix build .#runner-image --print-out-paths --no-link)" \
  docker://registry.example.com/nix-store-csi-runner:0.1.0
```

`nix build .#chart` packages the chart as a `.tgz`.

## Your own command

Put the program in `args`, and it or its store path in `closures`. Set `args`,
not `command`, so tini stays PID 1.

## Troubleshooting

| Symptom | Cause |
|---|---|
| Pod stuck in `ContainerCreating` with `driver name nix-store-csi not found` | The node plugin isn't running on that node, or the registrar can't reach kubelet's `plugins_registry`. |
| `not found in any store` in the pod's events | No configured cache has the path, or no trusted key signed its narinfo. |
| The command can't find a file | The file's store path isn't in the closure of anything in `closures`. |

## Not tested

Only the k3s VM test in `nix flake check` has installed the chart.
