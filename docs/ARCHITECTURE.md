# Architecture

nix-store-csi is a CSI node plugin that mounts Nix store paths in Kubernetes
pods. A pod lists store paths in an inline volume mounted at `/nix/store`. The
plugin fetches their closure from binary caches into a store shared by the node.
It then mounts a view that holds that closure and nothing else. The pod's image
needs no `/nix/store` of its own.

A volume can list store paths for each Nix system, and the plugin uses the list
for its node's system. Every view has a `.roots/` directory that links each
listed store path by its name without the hash. A pod's command can then use
the same path on every system.

![The plugin fetches from binary caches into the node store, hard-links a view, and bind-mounts it read-only at the pod's /nix/store](architecture.svg)

## Unpacking

A NAR comes from the network, and the plugin checks its NarHash only after
unpacking it into `tmp/`. The decompressor, the NAR parser and the writes run
on threads that Landlock confines to `tmp/`, where they can create directories,
files and symlinks and write to files they create. A bug that follows a
symlink in the NAR, or joins a bad name, then fails instead of writing outside
`tmp/`. The threads share the plugin's memory, so this doesn't contain code
that a crafted NAR manages to run. The transport reads the NAR on other
threads, since it reads credentials and may request the rest of a broken
download. It also undoes the compression that a cache adds to the response,
since a resume asks for the rest from an offset in the uncompressed NAR.

## Ranges

A closure's fetch ends about when its largest NAR's does, and a cache that
limits each connection, like S3, sends one NAR slower than the plugin
decompresses it. So from a cache that supports ranges, the plugin fetches a
compressed NAR in ranges, several at once, and decompresses them in order. It
adds a range each time decompression catches up with one that hasn't all
arrived, so a cache that keeps up gets two at a time. Over HTTP/2 the ranges
would share one connection, so after the first range the rest of the NAR comes
in one request. An uncompressed NAR comes in ranges only from an object store,
like S3, since another cache may compress it on the fly, which ranges would
lose.

Ranges that arrive early wait in memory. A NAR has at most 16 ranges in flight,
and the plugin at most 64 whatever `--max-substitution-jobs` is. A range counts
from its request until decompression has read it, so 8 MiB ranges take at most
512 MiB and 64 connections. Each range waits for the cache's first byte, which
S3 can take 100 ms to send, and smaller ranges spend more of their time
waiting. A NAR whose narinfo gives a `FileSize` of 8 MiB or less comes in one
request outside the 64, so it never waits while large ones
hold them all. Without a `FileSize`, the first range gives the size, so even a
small NAR takes one of the 64 for that first request.

The NAR still counts as one of `--max-substitution-jobs`. The jobs limit the
NARs being decompressed and unpacked, and ranges add none. The largest NARs
also start first, so if ranges took jobs, the NAR they help most would find
none free.

A range must come from the same version of the file as the first, by its
`ETag` or `Last-Modified`, or the NAR starts over.

## Syncing

A store path has to be on disk before the plugin can trust it after a reboot.
Syncing before each publish returns would make a cold publish wait for the
disk to write its whole closure. So as an optimization, a publish goes on once
the closure is in `store/`. Pods read from the page cache, so only losing power
can lose such a path, and that stops the pods too.

In the background, the plugin calls `syncfs` once for all the paths fetched by
then, and renames each narinfo from `<name>.unsynced` to `<name>.narinfo`.
Collection spares a path until its narinfo has its final name. If `syncfs`
fails, the plugin deletes the paths written before it, since the kernel reports
a lost write only once. Views may link those paths, so it also starts a new
generation of views. Publishes from then on build new views, and pods keep
their old ones until they unmount them.

At start-up the plugin compares the boot id with the one it recorded:

- On the same boot, after a restart of the plugin, it syncs and keeps the
  unsynced paths, which mounts may use.
- After a reboot or a failed sync, it deletes them, since they may have lost
  data, and starts a new generation of views.

Either way, it deletes any path in `store/` with no narinfo.

## Collection

Collection deletes the store paths that no mounted view, closure being fetched
or pending sync holds, and that no pod used within `--keep-recent`. It runs
every `--gc-interval`, 10 minutes by default. While free space or inodes on
the state dir's filesystem fall short of `--ensure-free`, it runs every minute
and also deletes recently used paths, least recently used first, until it
frees enough. Even then it spares the closures that the plugin prefetches,
since the plugin would fetch them again within a minute. A closure that leaves
the prefetch lists counts as used at that moment.

## Locality

The plugin doesn't talk to the API server. To let a scheduler prefer nodes
that hold a pod's closure, `locality` runs in a Deployment of its own as a
scheduler extender, and reads each node plugin's list of store paths. For each
node's system, it resolves the closure of the store paths that the pod's
volumes list from the caches. It scores the node by the share of that
closure's NAR size that the node store holds. Resolving can take longer than
the scheduler waits, so a closure that isn't resolved yet scores 0 while it
resolves in the background.

## Mountpoints

A container runtime creates a bind's mountpoint before it binds, and it cannot
do that in a read-only view. So views hold empty files and directories where
the runtime binds the node's store paths, like the driver and glibc that
NVIDIA's CDI spec binds on NixOS. They come from a list and from the node's
CDI specs, which the plugin reads every 10 seconds. A watch would miss specs
that change through symlinks, like NixOS's `/etc`, and spec dirs that systemd
creates again.

Mountpoints aren't part of a view. They don't change its hash and don't count
among its store paths. The plugin adds them to a view when it publishes it,
and to every view when the specs bring new ones, so a container that restarts
after a NixOS rebuild finds the new driver's. It never removes one, since a
container may be bound there. It never writes inside a store object of the
closure, since the runtime binds the node's copy of that store path over it.

## State dir structure

```
<state-dir>/
  lock                        held by the one process using the state dir
  version                     the version of the state dir's layout
  boot-id                     the boot that last used the state dir
  sync-failed                 left by a failed sync until the next start-up
  generation                  the generation of the views that publishes build
  store/<name>                unpacked store objects, verified
  narinfo/<name>.narinfo      a narinfo per store path, dated by last use, and
                              named <name>.unsynced until the path is synced
  views/<generation>-<hash>/  a closure's store objects, hard-linked from
                              store/, .roots/ and mountpoints. The hash covers
                              the closure and the roots.
  volumes/<volume>            which view the volume mounts, and where
  tmp/                        store objects being unpacked, views being built,
                              and trash being deleted
```

