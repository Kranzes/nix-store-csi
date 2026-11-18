# Using the driver in pods

A pod gets a closure through an inline volume of the `nix-store-csi` driver.
[DEPLOY.md](DEPLOY.md) covers installing the chart and its settings.

## Example

[hello-pod.yaml](../deploy/hello-pod.yaml) runs GNU hello on an amd64 or arm64 node:

```sh
kubectl apply -f https://raw.githubusercontent.com/Kranzes/nix-store-csi/master/deploy/hello-pod.yaml
kubectl logs hello
```

A pod stays in `ContainerCreating` until its closure is on the node. Past 90
seconds, its events show how many store paths and bytes have arrived.

## Multiple architectures

One manifest can serve nodes of multiple architectures. Add a
`storePaths.<system>` attribute for each Nix system. A node uses the one for
its system and falls back to `storePaths`. The store paths differ per system,
so `args` can't name one. It uses `/nix/store/.roots/<name>` instead, a link
that the plugin adds for each listed store path, named without the hash.
[hello-pod.yaml](../deploy/hello-pod.yaml) does this:

```yaml
spec:
  affinity:
    nodeAffinity:
      requiredDuringSchedulingIgnoredDuringExecution:
        nodeSelectorTerms:
          - matchExpressions:
              - key: kubernetes.io/arch
                operator: In
                values: [amd64, arm64]
  containers:
    - name: hello
      args: [/nix/store/.roots/hello-2.12.3/bin/hello]
      # ...
  volumes:
    - name: nix
      csi:
        driver: nix-store-csi
        readOnly: true
        volumeAttributes:
          storePaths.x86_64-linux: /nix/store/5z2yp3ysx8476c8g5w25b0smlgkjvaq3-hello-2.12.3
          storePaths.aarch64-linux: /nix/store/0xy6jlccm8kfpwmhkxabz3638lchyi74-hello-2.12.3
```

- The affinity keeps the pod off nodes that have no store paths for their
  system. A volume can't affect scheduling. Without the affinity, a pod on
  such a node stays in `ContainerCreating`, and its events show the error.
- Two store paths in one list can't have the same name, since they would need
  the same link.
- The name has the version in it, so `args` changes when the version does. To
  [inject the paths](#injecting-store-paths-into-manifests), add a variable for
  each system, like `HELLO_AMD64 = nixpkgs.legacyPackages.x86_64-linux.hello`,
  and get the link's name from `hello.name`.

## Injecting store paths into manifests

A nixpkgs update often changes store paths, so paths copied into manifests by
hand go stale. Instead, write a variable like
`${NIX_STORE_PATH_HELLO}` where a path goes and let Nix fill it in. In a pod
for one system, that changes two lines:

```yaml
args: [${NIX_STORE_PATH_HELLO}/bin/hello]
# ...
volumeAttributes:
  storePaths: ${NIX_STORE_PATH_HELLO}
```

[manifests.nix](../deploy/manifests.nix) fills in the variables. Each name in its
`storePaths` set becomes a `NIX_STORE_PATH_` variable.

- `unsafeDiscardStringContext` drops the derivation's dependency on the
  packages, so the build neither builds nor fetches them. The nodes fetch
  them, so they have to be in a cache the plugin uses.
- `envsubst` replaces only `NIX_STORE_PATH_` variables. Others in the
  manifests, like those in a shell script or Nix's own `NIX_PATH`, stay as
  they are. The build fails on a `NIX_STORE_PATH_` variable that `storePaths`
  doesn't set.
- The store paths match the system of the `pkgs` that calls the derivation.
  Pin pods to that architecture with a `nodeSelector` on
  `kubernetes.io/arch`, or list store paths for each system as in
  [Multiple architectures](#multiple-architectures).

CI can build the derivation and commit the result to a branch that Flux or
Argo CD syncs. The same variables work in Helm values files and other
text that ends up in a pod spec. Before it commits, CI can run
`nix-store-csi unpack --dry-run` on the store paths, with the chart's
`substituters` and `trustedPublicKeys`. It fails if the caches lack a store
path, so no pod waits for one.

The derivation's `prefetch` output is a values file with the chart's
`prefetch` value for the same store paths. Apply it in a commit or sync of its
own, and the manifests once the nodes hold it, as in
[Prefetching](DEPLOY.md#prefetching).

## Multiple programs on `PATH`

List one
[`buildEnv`](https://nixos.org/manual/nixpkgs/unstable/#sec-buildEnv) of the
packages, in [manifests.nix](../deploy/manifests.nix):

```nix
TOOLS = buildEnv {
  name = "tools";
  paths = [ hello curl ];
};
```

and put its `bin` on `PATH`:

```yaml
env:
  - name: PATH
    value: ${NIX_STORE_PATH_TOOLS}/bin
# ...
volumeAttributes:
  storePaths: ${NIX_STORE_PATH_TOOLS}
```

## GPU pods

How a pod finds the GPU driver depends on the node.

### NixOS nodes

With `hardware.nvidia-container-toolkit.enable`, NixOS writes a CDI spec to
`/var/run/cdi`. For a container that gets a GPU, the container runtime binds
the driver's store path, glibc's `lib` and `lib64` at their own store paths,
and `/run/opengl-driver`. Programs from nixpkgs find `libcuda.so` in
`/run/opengl-driver/lib`, whose links point into the driver's store path.

The runtime cannot create those binds' mountpoints in the read-only volume, so
the container fails to start with `read-only file system`. Turn on
`mountpoints.cdi`:

```yaml
# values.yaml
mountpoints:
  cdi: true
```

Each volume then has an empty directory or file at each path under
`/nix/store` that a spec in the node's `/etc/cdi` or `/var/run/cdi` binds, and
every pod on the node sees them. The plugin reads the specs every 10 seconds,
and adds new mountpoints to the volumes that are already mounted. So after a
NixOS rebuild changes the driver or glibc, a container that restarts in a
running pod finds the new ones. The plugin never removes a mountpoint.

- A bind gets a mountpoint of the type of its host path, once that path exists.
  A mountpoint can't change later, so the plugin doesn't guess.
- A mount that isn't a bind, like a tmpfs, gets a directory.
- A path in a store path that the pod's closure has gets none. The plugin never
  changes a store object, and the runtime binds the node's copy of the store
  path over the closure's.

For a runtime or hook that binds under `/nix/store` without CDI, list the paths
in `mountpoints.paths`.

### Other nodes

The NVIDIA Container Toolkit binds the driver's libraries at their paths on the
node, in a dir like `/usr/lib/x86_64-linux-gnu` or `/usr/lib64`. Programs from
nixpkgs don't search there, and nixpkgs' glibc doesn't read the
`/etc/ld.so.cache` that the toolkit updates. Set `LD_LIBRARY_PATH` to the dir,
or add it to the RUNPATH of the library that loads `libcuda.so`, like
`libcudart.so`, with `patchelf --add-rpath`. The loader searches
`LD_LIBRARY_PATH` before RUNPATH, so it suits only an image with no libraries
of its own in that dir, like the runner image.

### What is tested

The VM test covers NixOS nodes with stand-ins for the driver and glibc.
containerd and runc bind them into containers on a published volume, from a
CDI spec like NixOS's and from `mountpoints.paths`, and crictl starts the
containers. After the spec changes, a new container in the same pod gets the
new binds. Nothing tests a GPU, NixOS's generated spec, a device plugin, or
the steps for other nodes.

## Your own image

The example runs in the runner image, which has tini as its entrypoint, an
empty `/nix/store`, users, `/tmp` and nixpkgs' CA certificates, with
`SSL_CERT_FILE` and `NIX_SSL_CERT_FILE` set to them. It runs as nobody, whose
home is `/home/nobody`. Any other image works if it meets these requirements:

- Nothing in the image may need its own `/nix/store`, since the volume hides
  it. An image without `/nix/store` is fine, because the container runtime
  creates the mount point.
- An init is optional if the program exits on SIGTERM and either starts no
  child processes or reaps the ones it starts, as most servers do. Otherwise
  deleting the pod waits out its grace period, and finished children pile up
  as zombies. Then use the image's own init, or run tini from the closure. Put
  it in `command`, which replaces the image's entrypoint, and in `storePaths`:

  ```yaml
  command: [${NIX_STORE_PATH_TINI}/bin/tini, -s, --]
  args: [${NIX_STORE_PATH_HELLO}/bin/hello]
  # ...
  volumeAttributes:
    storePaths: ${NIX_STORE_PATH_TINI} ${NIX_STORE_PATH_HELLO}
  ```

  Add `TINI = tini` to `storePaths` in [manifests.nix](../deploy/manifests.nix).
- The image provides what the program expects from its environment, like users
  in `/etc/passwd`, `/tmp` and CA certificates.

Keep store paths out of the image, and the program's store path only in `args`
and `storePaths`. The image then doesn't change when the program does. The
runner image is built this way from [runner-image.nix](../deploy/runner-image.nix), which
copies what it needs instead of linking into the store. To add a company CA,
override it with PEM files:

```nix
nix-store-csi.packages.${system}.runner-image.override {
  name = "my-runner";
  tag = "1";
  extraCertificates = [ ./company-ca.pem ];
}
```

To run an existing image built with `dockerTools.buildLayeredImage`, set
`includeStorePaths = false`. The image then holds only links into `/nix/store`,
and `storePaths` has to list the store paths it links to, and those that its
`Env` names, like `cacert` in `SSL_CERT_FILE=${cacert}/etc/ssl/certs/ca-bundle.crt`.
Rebuild the image when those paths change.
