# Deploying

## Install

The node plugin runs privileged, so its namespace has to allow privileged pods.
Pods that use its volumes pass the restricted Pod Security level.

```sh
kubectl create namespace nix-store-csi-system
kubectl label namespace nix-store-csi-system pod-security.kubernetes.io/enforce=privileged
helm install nix-store-csi oci://ghcr.io/kranzes/charts/nix-store-csi -n nix-store-csi-system
```

## A disk for the store

Closures like CUDA's run to tens of gigabytes. On the node's root disk the
store competes with images and logs, and kubelet can't tell what it's using.
Mount a disk of its own on every node and point `stateDir` at it. Keep the whole
state dir there, since views are hard links into `store/`.

## Example

[hello-pod.yaml](hello-pod.yaml) runs GNU hello on an amd64 node:

```sh
kubectl apply -f deploy/hello-pod.yaml
kubectl logs hello
```

A pod stays in `ContainerCreating` until its closure is on the node. Past 90
seconds, its events show how much has arrived.
