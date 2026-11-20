// A model of the node store in rust/src/store: ensures, fetches, syncs,
// garbage collection, publishes, crashes and start-ups, as they interleave.
//
// What it covers:
//
//   ensure       Caches::resolve (closure.rs), Store::ensure, hold,
//                fetch_shared, check_references (fetch.rs)
//   fetch        install, fetch (fetch.rs)
//   sync         Store::sync, forget, delete (mod.rs)
//   collect      Store::collect, drop_unmounted, drop_unused (gc.rs, views.rs)
//   publish      Store::view, mounts::publish
//   unpublish    mounts::unpublish, Store::drop_view
//   start-up     Store::new, settle, prune_views
//
// What it leaves out, since no invariant below depends on it: signatures,
// which cache serves a NAR, size limits, --keep-recent and last uses
// (collection deletes any paths it may), job slots, mountpoints, metrics,
// and errors from the filesystem other than a failed syncfs.
//
// What it assumes:
//
//   - The filesystem keeps the order of metadata operations, so a power loss
//     leaves the names as they were at some earlier moment. `powerLoss` can
//     fire after any step, which stands for a power loss whose last commit
//     was then.
//   - Only a syncfs makes a store object's data durable.
//   - A resolution reads all its narinfos at once.
//
// `step` takes the small steps of the code, for the model checker. TLC
// visits every state of two small instances. `caches` has two ensures, two
// caches that disagree, and no fault. `faults` has one path, a failed syncfs
// and two crashes. `replay` takes the steps that rust/src/store/model.rs can
// make the plugin take, one whole call at a time, and the test there replays
// its traces of `sample`.
//
// When the code in the files above changes how it guards a path, change the
// model first and check it.

module store {
  type Path = str
  type Volume = str
  type EnsureId = str

  /// The store paths that exist, and the references that the caches'
  /// narinfos give each. Two caches can disagree on a path's references.
  const PATHS: Set[Path]
  const REFERENCES: Path -> Set[Set[Path]]
  /// The volumes that kubelet may publish.
  const VOLUMES: Set[Volume]
  /// Names for the ensures that may run at once.
  const ENSURES: Set[EnsureId]
  /// How many kills and power losses a run may have, and how many failed
  /// syncfs calls.
  const MAX_CRASHES: int
  const MAX_FAILURES: int

  /// Where a store object's data is. `Fresh` data is only in the page cache,
  /// a syncfs made `Durable` data safe, and `Lost` data may be gone, after a
  /// failed syncfs or a power loss.
  type Data = Fresh | Durable | Lost

  type Phase =
    | Queued     // waits for a job
    | Unpacking  // read failed_syncs and writes into tmp/
    | Marked     // in Store::unsynced
    | Written    // its narinfo is in narinfo/, named .unsynced

  /// `refs` holds the references in the narinfo that the fetch writes. The
  /// ensure that made the fetch resolved that narinfo.
  type Fetch = { phase: Phase, since: int, data: Data, refs: Set[Path] }

  type Progress =
    | Resolved  // has its closure, from narinfos it read with no lock
    | Held      // holds the closure, and knows which paths the store lacks
    | Joined    // has made or joined the fetches of those paths

  /// A Store::ensure. Once `Held`, its hold on the closure lasts while it
  /// is in `State.ensures`, which is until its task has ended and the
  /// caller has dropped the Ensuring.
  type Ensure = {
    root: Path,
    // The closure it resolved, and the references of each path in it.
    paths: Set[Path],
    refs: Path -> Set[Path],
    progress: Progress,
    // The generation in which it took its hold. A publish refuses the
    // closure in a later one.
    heldAt: int,
    // The fetches it still waits for.
    waiting: Set[Path],
    // Whether a fetch wrote a narinfo since it began, so that it checks the
    // closure's narinfos when its fetches end.
    installed: bool,
    failed: bool,
    caller: bool,
  }

  /// A view, named by its generation, its root and the closure it holds. A
  /// volume lists one root here.
  type View = { generation: int, root: Path, paths: Set[Path] }

  /// Store::sync, which one task runs at a time. Its batch holds the paths
  /// it syncs, each with the mark it had. While `Forgetting`, it holds the
  /// `gc` lock.
  type SyncStage = Resting | Snapshotted | Flushed | Forgetting
  type Sync = { stage: SyncStage, batch: Path -> int }

  /// Store::collect inside `exclusively`.
  type Collection = { sweeping: bool, inUse: Set[Path], doomed: Set[Path] }

  /// Store::new and prune_views, by what a plugin that is starting does
  /// next. It serves once it is `Up`.
  type StartStage =
    | Up
    | Scanning   // reads what the last run left, and syncs it
    | Settling   // renames or deletes narinfos, deletes paths, then writes
                 // the generation
    | Clearing   // deletes the record of a failed sync
    | Recording  // writes the boot id
    | Pruning    // forgets the volumes that are no longer mounted

  type Startup = {
    stage: StartStage,
    // Whether it keeps the paths that the last run left unsynced.
    keeps: bool,
    // The narinfos named .unsynced that it has yet to rename or delete.
    todo: Set[Path],
    // Whether it deleted a path.
    dropped: bool,
  }

  /// A publish that has the view in views/. It then writes the volume's
  /// record, with views/ still locked, and binds the view at the target.
  type Publishing = { view: View, recorded: bool }

  type State = {
    // The state dir.
    store: Set[Path],
    synced: Set[Path],
    unsynced: Set[Path],
    views: Set[View],
    volumes: Volume -> View,
    // The references in the narinfo of each path that has one.
    references: Path -> Set[Path],
    generationRecord: int,
    syncFailed: bool,
    // Whether the boot id record names this boot.
    bootRecorded: bool,
    // The plugin's memory.
    generation: int,
    marks: Path -> int,
    failedSyncs: int,
    fetches: Path -> Fetch,
    ensures: EnsureId -> Ensure,
    sync: Sync,
    // The sync tasks that wait for their turn, by the path each is for.
    syncs: Set[Path],
    collection: Collection,
    // The publishes and unpublishes under way. Each has its volume's turn
    // and the `gc` lock, shared.
    publishing: Volume -> Publishing,
    unpublishing: Set[Volume],
    startup: Startup,
    // The node. The view bound at each volume's target.
    bound: Volume -> View,
    // What only the model sees.
    data: Path -> Data,
    // Views with a link to a store object that left store/.
    stale: Set[View],
    // Whether a publish gave a pod a view of lost data.
    servedLost: bool,
    crashes: int,
    failures: int,
  }

  pure val RESTING: Sync = { stage: Resting, batch: Map() }

  pure val OFF: Collection = { sweeping: false, inUse: Set(), doomed: Set() }

  pure val UP: Startup =
    { stage: Up, keeps: false, todo: Set(), dropped: false }

  pure def has(m: a -> b, k: a): bool = m.keys().contains(k)

  pure def without(m: a -> b, ks: Set[a]): a -> b =
    m.keys().exclude(ks).mapBy(k => m.get(k))

  pure def mapValues(m: a -> b, f: b => b): a -> b =
    m.keys().mapBy(k => f(m.get(k)))

  /// The narinfos that a resolution may find in the caches, one for each
  /// path.
  pure val CHOICES: Set[Path -> Set[Path]] =
    PATHS.setOfMaps(PATHS.powerset()).filter(choice =>
      PATHS.forall(p => REFERENCES.get(p).contains(choice.get(p))))

  pure def held(s: State): Set[Path] =
    s.ensures.keys().filter(e => s.ensures.get(e).progress != Resolved)
      .map(e => s.ensures.get(e).paths).flatten()

  pure def linked(views: Set[View]): Set[Path] =
    views.map(v => v.paths).flatten()

  /// The paths that an ensure holds or a view links.
  pure def inUse(s: State): Set[Path] = linked(s.views).union(held(s))

  /// The paths with a narinfo under either name.
  pure def narinfos(s: State): Set[Path] = s.synced.union(s.unsynced)

  /// Deletes the narinfos of `paths`, under either name.
  pure def unlinked(s: State, paths: Set[Path]): State = {
    ...s,
    synced: s.synced.exclude(paths),
    unsynced: s.unsynced.exclude(paths),
    references: s.references.without(paths),
  }

  /// Gives the narinfos of `paths` their final names.
  pure def renamed(s: State, paths: Set[Path]): State = {
    ...s,
    unsynced: s.unsynced.exclude(paths),
    synced: s.synced.union(paths),
  }

  /// Whether `paths` holds every reference of each path in it, as the node
  /// store's narinfos give them.
  pure def closed(s: State, paths: Set[Path]): bool =
    paths.forall(p =>
      narinfos(s).contains(p) and s.references.get(p).subseteq(paths))

  pure def lost(s: State, p: Path): bool =
    s.marks.has(p) and s.marks.get(p) < s.failedSyncs

  /// Whether a publish, an unpublish or an ensure can take the `gc` lock,
  /// which they share. A plugin that is starting takes no call, and a
  /// collection or a `forget` holds the lock.
  pure def shared(s: State): bool = and {
    s.startup.stage == Up,
    not(s.collection.sweeping),
    s.sync.stage != Forgetting,
  }

  /// Whether a collection or a `forget` can take the `gc` lock. They wait
  /// for the publishes and unpublishes under way.
  pure def exclusive(s: State): bool =
    shared(s) and s.publishing == Map() and s.unpublishing == Set()

  /// Whether the lock on views/ is free. A publish holds it until it has
  /// written its record.
  pure def viewsFree(s: State): bool =
    s.publishing.keys().forall(v => s.publishing.get(v).recorded)

  /// Whether the volume's turn and the lock on views/ are free.
  pure def turn(s: State, v: Volume): bool = and {
    not(s.publishing.has(v)),
    not(s.unpublishing.contains(v)),
    viewsFree(s),
  }

  /// Whether the volume is bound to the view in its record.
  pure def mounted(s: State, v: Volume): bool =
    s.volumes.has(v) and s.bound.has(v) and s.bound.get(v) == s.volumes.get(v)

  /// Store::evict for each of `paths`.
  pure def evicted(s: State, paths: Set[Path]): State = {
    val gone = s.store.intersect(paths)
    { ...s,
      store: s.store.exclude(gone),
      data: s.data.without(gone),
      stale: s.stale.union(s.views.filter(v => v.paths.intersect(gone) != Set())) }
  }

  /// Store::delete. It holds the lock on the marks from the eviction until
  /// the narinfo is gone. A fetch marks its path under that lock before it
  /// writes the narinfo, so it writes none in between.
  pure def deleted(s: State, p: Path): State = {
    val t = unlinked(evicted(s, Set(p)), Set(p))
    { ...t, marks: t.marks.without(Set(p)) }
  }

  /// Forgets the views other than `kept`.
  pure def keeping(s: State, kept: Set[View]): State = {
    ...s,
    views: s.views.intersect(kept),
    stale: s.stale.intersect(kept),
  }

  // ensure

  /// Whether its task has ended.
  pure def ended(x: Ensure): bool = x.progress == Joined and x.waiting == Set()

  pure def done(x: Ensure): bool = ended(x) and not(x.failed)

  /// Drops the ensures that hold nothing any more.
  pure def settledEnsures(ensures: EnsureId -> Ensure): EnsureId -> Ensure =
    ensures.without(ensures.keys().filter(e =>
      not(ensures.get(e).caller) and ended(ensures.get(e))))

  /// Store::check_references, for an ensure whose fetches have all ended. A
  /// path may have come from another narinfo than the ensure resolved, or
  /// have lost its narinfo.
  pure def concluded(s: State, x: Ensure): Ensure =
    if (not(ended(x))) x
    else { ...x,
      installed: false,
      failed: x.installed and not(closed(s, x.paths)) }

  /// The fetch of `p` ended, and each ensure that joined it gets the result.
  pure def fetchEnded(s: State, p: Path, ok: bool): State = {
    val heard = s.ensures.mapValues(x =>
      if (x.progress != Joined or not(x.waiting.contains(p))) x
      else if (ok) concluded(s, { ...x, waiting: x.waiting.exclude(Set(p)) })
      else { ...x, waiting: Set(), installed: false, failed: true })
    { ...s, fetches: s.fetches.without(Set(p)), ensures: settledEnsures(heard) }
  }

  pure def canResolve(s: State, e: EnsureId): bool =
    s.startup.stage == Up and not(s.ensures.has(e))

  /// Caches::resolve. Each narinfo comes from the node store if it has one,
  /// or else from the caches, where `choice` says which they give.
  pure def ensureResolved(
    s: State, e: EnsureId, root: Path, choice: Path -> Set[Path]
  ): State = {
    val refs = PATHS.mapBy(p =>
      if (narinfos(s).contains(p)) s.references.get(p) else choice.get(p))
    val paths = PATHS.fold(Set(root), (found, _) =>
      found.union(found.map(p => refs.get(p)).flatten()))
    { ...s, ensures: s.ensures.put(e, {
      root: root,
      paths: paths,
      refs: paths.mapBy(p => refs.get(p)),
      progress: Resolved,
      heldAt: 0,
      waiting: Set(),
      installed: false,
      failed: false,
      caller: true,
    }) }
  }

  pure def canHold(s: State, e: EnsureId): bool = and {
    shared(s),
    s.ensures.has(e),
    s.ensures.get(e).progress == Resolved,
  }

  pure def ensureHeld(s: State, e: EnsureId): State = {
    ...s,
    ensures: s.ensures.setBy(e, x => { ...x,
      progress: Held,
      heldAt: s.generation,
      waiting: x.paths.exclude(s.store) }),
  }

  pure def canJoin(s: State, e: EnsureId): bool =
    s.ensures.has(e) and s.ensures.get(e).progress == Held

  pure def ensureJoined(s: State, e: EnsureId): State = {
    val x = s.ensures.get(e)
    val started = x.waiting.exclude(s.fetches.keys())
    val fetches = s.fetches.keys().union(started).mapBy(p =>
      if (started.contains(p))
        { phase: Queued, since: 0, data: Fresh, refs: x.refs.get(p) }
      else s.fetches.get(p))
    { ...s,
      fetches: fetches,
      ensures: settledEnsures(
        s.ensures.put(e, concluded(s, { ...x, progress: Joined }))) }
  }

  /// A whole Store::ensure, up to its fetches.
  pure def ensured(
    s: State, e: EnsureId, root: Path, choice: Path -> Set[Path]
  ): State =
    ensureJoined(ensureHeld(ensureResolved(s, e, root, choice), e), e)

  pure def canRelease(s: State, e: EnsureId): bool =
    s.ensures.has(e) and s.ensures.get(e).caller

  pure def released(s: State, e: EnsureId): State = {
    ...s,
    ensures: settledEnsures(s.ensures.setBy(e, x => { ...x, caller: false })),
  }

  // fetch

  pure def inPhase(s: State, p: Path, phase: Phase): bool =
    s.fetches.has(p) and s.fetches.get(p).phase == phase

  /// Another fetch may have brought the path in while this one waited.
  pure def fetchBegun(s: State, p: Path): State =
    if (s.store.contains(p)) fetchEnded(s, p, true)
    else { ...s, fetches: s.fetches.setBy(p, f =>
      { ...f, phase: Unpacking, since: s.failedSyncs }) }

  pure def fetchMarked(s: State, p: Path): State = {
    ...s,
    marks: s.marks.put(p, s.fetches.get(p).since),
    fetches: s.fetches.setBy(p, f => { ...f, phase: Marked }),
  }

  pure def fetchWritten(s: State, p: Path): State = {
    ...s,
    unsynced: s.unsynced.union(Set(p)),
    references: s.references.put(p, s.fetches.get(p).refs),
    fetches: s.fetches.setBy(p, f => { ...f, phase: Written }),
  }

  pure def fetchMovedIn(s: State, p: Path): State = {
    val t = { ...s,
      store: s.store.union(Set(p)),
      data: s.data.put(p, s.fetches.get(p).data),
      syncs: s.syncs.union(Set(p)),
      ensures: s.ensures.mapValues(x =>
        if (ended(x)) x else { ...x, installed: true }) }
    fetchEnded(t, p, true)
  }

  /// The rest of a fetch that is unpacking.
  pure def fetchInstalled(s: State, p: Path): State =
    fetchMovedIn(fetchWritten(fetchMarked(s, p), p), p)

  pure def canFail(s: State, p: Path): bool =
    inPhase(s, p, Queued) or inPhase(s, p, Unpacking)

  // sync

  pure def canStartSync(s: State, p: Path): bool =
    s.sync.stage == Resting and s.syncs.contains(p)

  pure def syncSnapshotted(s: State): State = {
    ...s,
    sync: { ...RESTING,
      stage: Snapshotted,
      batch: s.marks.keys().intersect(s.store).mapBy(p => s.marks.get(p)) },
  }

  /// A sync whose path an earlier sync handled returns at once.
  pure def syncStarted(s: State, p: Path): State = {
    val t = { ...s, syncs: s.syncs.exclude(Set(p)) }
    if (s.marks.has(p)) syncSnapshotted(t) else t
  }

  pure def reaches(data: Path -> Data, reached: Data): Path -> Data =
    data.mapValues(d => if (d == Fresh) reached else d)

  pure def canFlush(s: State, ok: bool): bool =
    s.sync.stage == Snapshotted and (ok or s.failures < MAX_FAILURES)

  /// The syncfs. A failed one may have lost any data not yet durable, and
  /// that includes what fetches have unpacked so far.
  pure def syncFlushed(s: State, ok: bool): State = {
    val t = { ...s, sync: { ...s.sync, stage: Flushed } }
    if (ok) { ...t, data: reaches(s.data, Durable) }
    else { ...t,
      data: reaches(s.data, Lost),
      fetches: s.fetches.mapValues(f =>
        if (f.phase == Queued) f else { ...f, data: Lost }),
      failedSyncs: s.failedSyncs + 1,
      syncFailed: true,
      failures: s.failures + 1 }
  }

  /// The paths of the batch with marks from before a failed sync. After a
  /// syncfs that failed, that is all of them.
  pure def lostBatch(s: State): Set[Path] =
    s.sync.batch.keys().filter(p => s.sync.batch.get(p) < s.failedSyncs)

  /// Store::forget waits for the publishes and the collection under way.
  pure def canForget(s: State): bool = and {
    s.sync.stage == Flushed,
    exclusive(s),
    lostBatch(s) != Set(),
  }

  /// Views may link the lost paths, so new publishes get a new generation.
  pure def syncForgetBegun(s: State): State = {
    ...s,
    generation: s.generation + 1,
    sync: { ...s.sync, stage: Forgetting },
  }

  pure def canForgetPath(s: State, p: Path): bool =
    s.sync.stage == Forgetting and lostBatch(s).contains(p)

  /// Deletes one lost path, and gives up the lock after the last.
  pure def syncForgot(s: State, p: Path): State = {
    val t = deleted(s, p)
    val sync = { ...s.sync, batch: s.sync.batch.without(Set(p)) }
    val u = { ...t, sync: sync }
    if (lostBatch(u) == Set()) { ...u, sync: { ...sync, stage: Flushed } }
    else u
  }

  pure def canRename(s: State): bool =
    s.sync.stage == Flushed and lostBatch(s) == Set()

  pure def syncRenamed(s: State): State = {
    val paths = s.sync.batch.keys()
    { ...renamed(s, paths.intersect(s.unsynced)),
      marks: s.marks.without(paths),
      sync: RESTING }
  }

  /// A whole sync needs the `gc` lock in case it has paths to forget.
  pure def canSync(s: State, ok: bool): bool = and {
    s.sync.stage == Resting,
    exclusive(s),
    s.syncs != Set(),
    ok or s.failures < MAX_FAILURES,
  }

  /// The sync that a task has started, through to its end.
  pure def syncRun(s: State, ok: bool): State =
    if (s.sync.stage != Snapshotted) s
    else {
      val flushed = syncFlushed(s, ok)
      val lost = lostBatch(flushed)
      syncRenamed(
        if (lost == Set()) flushed
        else lost.fold(syncForgetBegun(flushed), (t, p) => syncForgot(t, p)))
    }

  /// Every waiting sync task takes its turn. The first one syncs, and the
  /// others find their paths handled.
  pure def syncedAll(s: State, ok: bool): State =
    s.syncs.fold(s, (t, p) => syncRun(syncStarted(t, p), ok))

  // collect

  /// Store::drop_unmounted. It forgets the volumes that aren't bound to the
  /// view in their record, and deletes the views that no other volume uses.
  pure def pruned(s: State): State = {
    val volumes = s.volumes.keys().filter(v => mounted(s, v))
    { ...keeping(s, volumes.map(v => s.volumes.get(v))),
      volumes: volumes.mapBy(v => s.volumes.get(v)) }
  }

  /// The paths that a collection starting in `s` may delete.
  pure def collectable(s: State): Set[Path] = {
    val t = pruned(s)
    t.store.exclude(inUse(t)).exclude(t.marks.keys())
  }

  pure def collectionBegun(s: State, doomed: Set[Path]): State = {
    val t = pruned(s)
    { ...t, collection: { sweeping: true, inUse: inUse(t), doomed: doomed } }
  }

  pure def collectionDeleted(s: State, p: Path): State = {
    ...deleted(s, p),
    collection: { ...s.collection, doomed: s.collection.doomed.exclude(Set(p)) },
  }

  pure def canEndCollection(s: State): bool =
    s.collection.sweeping and s.collection.doomed == Set()

  /// Deletes the narinfos that a crash left without their paths.
  pure def collectionEnded(s: State): State = {
    val orphans = narinfos(s)
      .exclude(s.collection.inUse).exclude(s.marks.keys()).exclude(s.store)
    { ...unlinked(s, orphans), collection: OFF }
  }

  /// A whole collection that deletes every path it may.
  pure def collected(s: State): State = {
    val t = collectionBegun(s, collectable(s))
    collectionEnded(t.collection.doomed.fold(t, (u, p) => collectionDeleted(u, p)))
  }

  // publish and unpublish

  pure def canPublish(s: State, v: Volume, e: EnsureId): bool = and {
    shared(s),
    turn(s, v),
    s.ensures.has(e),
    s.ensures.get(e).caller,
    done(s.ensures.get(e)),
  }

  /// Store::view up to the view's place in views/. A view that is there
  /// gets the volume's record at once. A refused or failed publish changes
  /// nothing.
  pure def publishStarted(s: State, v: Volume, e: EnsureId): State = {
    val x = s.ensures.get(e)
    val paths = x.paths
    val view = { generation: s.generation, root: x.root, paths: paths }
    val shares = s.views.contains(view)
    // A `forget` has deleted paths since the ensure checked its closure, or
    // a failed sync missed a path that a fetch was still unpacking.
    val refused = x.heldAt != s.generation or paths.exists(p => lost(s, p))
    if (mounted(s, v) or refused or not(shares or paths.subseteq(s.store))) s
    else { ...s,
      views: s.views.union(Set(view)),
      volumes: if (shares) s.volumes.put(v, view) else s.volumes,
      publishing: s.publishing.put(v, { view: view, recorded: shares }),
      servedLost: s.servedLost or s.stale.contains(view)
        or paths.exists(p => s.store.contains(p) and s.data.get(p) == Lost) }
  }

  pure def publishes(s: State, v: Volume, recorded: bool): bool =
    s.publishing.has(v) and s.publishing.get(v).recorded == recorded

  pure def publishRecorded(s: State, v: Volume): State = {
    ...s,
    volumes: s.volumes.put(v, s.publishing.get(v).view),
    publishing: s.publishing.setBy(v, x => { ...x, recorded: true }),
  }

  /// mounts::publish.
  pure def publishBound(s: State, v: Volume): State = {
    ...s,
    bound: s.bound.put(v, s.publishing.get(v).view),
    publishing: s.publishing.without(Set(v)),
  }

  /// A whole publish.
  pure def published(s: State, v: Volume, e: EnsureId): State = {
    val t = publishStarted(s, v, e)
    if (t.publishing.has(v)) publishBound(publishRecorded(t, v), v) else t
  }

  pure def canUnpublish(s: State, v: Volume): bool = and {
    shared(s),
    turn(s, v),
    s.volumes.has(v) or s.bound.has(v),
  }

  /// mounts::unpublish.
  pure def unpublishStarted(s: State, v: Volume): State = {
    ...s,
    bound: s.bound.without(Set(v)),
    unpublishing: s.unpublishing.union(Set(v)),
  }

  pure def canDrop(s: State, v: Volume): bool =
    s.unpublishing.contains(v) and viewsFree(s)

  /// Store::drop_view, which deletes the volume's view unless another
  /// volume's record names it.
  pure def unpublishDropped(s: State, v: Volume): State = {
    val volumes = s.volumes.without(Set(v))
    val used = volumes.keys().map(u => volumes.get(u))
    val unused =
      if (s.volumes.has(v)) Set(s.volumes.get(v)).exclude(used) else Set()
    { ...keeping(s, s.views.exclude(unused)),
      volumes: volumes,
      unpublishing: s.unpublishing.exclude(Set(v)) }
  }

  /// A whole unpublish.
  pure def unpublished(s: State, v: Volume): State =
    unpublishDropped(unpublishStarted(s, v), v)

  // crash and start-up

  /// A kill, or with `powerLoss` a reboot, which also takes the mounts and
  /// any data not yet durable. Either way the plugin starts again.
  pure def crashed(s: State, powerLoss: bool): State = {
    val t = { ...s,
      marks: Map(),
      failedSyncs: 0,
      fetches: Map(),
      ensures: Map(),
      sync: RESTING,
      syncs: Set(),
      collection: OFF,
      publishing: Map(),
      unpublishing: Set(),
      startup: { ...UP, stage: Scanning },
      crashes: s.crashes + 1 }
    if (powerLoss)
      { ...t, bound: Map(), data: reaches(s.data, Lost), bootRecorded: false }
    else t
  }

  /// Whether Store::settle has anything to sync.
  pure def pending(s: State): bool = s.syncFailed or s.unsynced != Set()

  pure def canScan(s: State, flushed: bool): bool = and {
    s.startup.stage == Scanning,
    flushed or (pending(s) and s.failures < MAX_FAILURES),
  }

  /// settle runs its syncfs only with something to settle, and `flushed`
  /// says whether that syncfs works. A failed one leaves its record, like a
  /// failed sync does.
  pure def startScanned(s: State, flushed: bool): State = {
    val failed = pending(s) and not(flushed)
    { ...s,
      data:
        if (pending(s)) reaches(s.data, if (failed) Lost else Durable)
        else s.data,
      failures: if (failed) s.failures + 1 else s.failures,
      // The next start-up's syncfs won't report the loss again.
      syncFailed: s.syncFailed or failed,
      startup: {
        stage: Settling,
        keeps: s.bootRecorded and not(s.syncFailed) and not(failed),
        todo: s.unsynced,
        dropped: false,
      } }
  }

  /// Renames the narinfo of an unsynced path that start-up keeps, and
  /// deletes any other, under either name.
  pure def startNarinfoSettled(s: State, p: Path): State = {
    val t =
      if (s.startup.keeps and s.store.contains(p)) renamed(s, Set(p))
      else unlinked(s, Set(p))
    { ...t, startup: { ...s.startup, todo: s.startup.todo.exclude(Set(p)) } }
  }

  /// The paths in store/ that settle deletes, once it is through with the
  /// narinfos.
  pure def droppable(s: State): Set[Path] =
    if (s.startup.stage == Settling and s.startup.todo == Set())
      s.store.exclude(s.synced)
    else Set()

  pure def startDropped(s: State, p: Path): State = {
    ...evicted(s, Set(p)),
    startup: { ...s.startup, dropped: true },
  }

  pure def canNumber(s: State): bool = and {
    s.startup.stage == Settling,
    s.startup.todo == Set(),
    droppable(s) == Set(),
  }

  /// Keeps the generation, or starts one that no view has.
  pure def startNumbered(s: State): State = {
    val newest = s.views.fold(s.generationRecord, (g, v) =>
      if (v.generation > g) v.generation else g)
    val generation =
      if (s.startup.keeps and not(s.startup.dropped)) s.generationRecord
      else newest + 1
    { ...s,
      generation: generation,
      generationRecord: generation,
      startup: { ...s.startup, stage: Clearing } }
  }

  pure def startCleared(s: State): State =
    { ...s, syncFailed: false, startup: { ...s.startup, stage: Recording } }

  pure def startRecorded(s: State): State =
    { ...s, bootRecorded: true, startup: { ...s.startup, stage: Pruning } }

  pure def startPruned(s: State): State = { ...pruned(s), startup: UP }

  /// A whole start-up.
  pure def startedUp(s: State, flushed: bool): State = {
    val scanned = startScanned(s, flushed)
    val settled = scanned.startup.todo.fold(scanned, (t, p) =>
      startNarinfoSettled(t, p))
    val dropped = droppable(settled).fold(settled, (t, p) => startDropped(t, p))
    startPruned(startRecorded(startCleared(startNumbered(dropped))))
  }

  pure def restarted(s: State): State = startedUp(crashed(s, false), true)

  pure def rebooted(s: State): State = startedUp(crashed(s, true), true)

  /// A crash of a plugin that serves an empty state dir starts up to the
  /// same state, so it would only use up the crashes.
  pure def canCrash(s: State): bool = and {
    s.crashes < MAX_CRASHES,
    or {
      s.startup.stage != Up,
      s.fetches.keys() != Set(),
      s.store != Set(),
      narinfos(s) != Set(),
      s.views != Set(),
      s.syncFailed,
    },
  }

  var s: State

  // Invariants

  /// A plugin that is starting has yet to settle what a crash left, so some
  /// invariants hold only once it serves.
  val serving = s.startup.stage == Up

  /// A name in store/ means a store object with a narinfo.
  val everyPathHasNarinfo = serving implies s.store.subseteq(narinfos(s))

  /// A narinfo with its final name alone means that the store object is on
  /// disk. Start-up keeps such a path after a reboot.
  val syncedMeansDurable = s.store.forall(p =>
    (s.synced.contains(p) and not(s.unsynced.contains(p)))
      implies s.data.get(p) == Durable)

  /// The plugin knows each store object that may have lost data, by its mark
  /// from before a failed sync.
  val lostDataIsMarked = serving implies
    s.store.forall(p => s.data.get(p) == Lost implies lost(s, p))

  /// No publish gives a pod a view of lost data, other than the view the pod
  /// already had.
  val neverServesLostData = not(s.servedLost)

  /// A view that a new publish may share links what store/ holds.
  val currentViewsLinkTheStore = serving implies s.views.forall(v =>
    v.generation == s.generation implies not(s.stale.contains(v)))

  /// A view that a new publish may share holds every reference of each
  /// store object in it.
  val currentViewsAreClosed = serving implies s.views.forall(v =>
    v.generation == s.generation implies closed(s, v.paths))

  /// Collection deletes no path that an ensure holds, a view links or a sync
  /// has yet to make durable.
  val collectionSparesPathsInUse =
    s.collection.doomed.intersect(inUse(s).union(s.marks.keys())) == Set()

  /// A mounted volume has its record, and its view is in views/, so
  /// collection sees both.
  val boundVolumesHaveViews = s.bound.keys().forall(v =>
    mounted(s, v) and s.views.contains(s.bound.get(v)))

  /// The store keeps a closure that an ensure fetched and still holds, with
  /// every reference of each store object in it, until a `forget` starts a
  /// new generation.
  val heldClosuresStay = s.ensures.keys().forall(e => {
    val x = s.ensures.get(e)
    (done(x) and x.heldAt == s.generation)
      implies (x.paths.subseteq(s.store) and closed(s, x.paths))
  })

  /// A path that still waits for its sync is marked, so collection spares
  /// it.
  val unsyncedPathsAreMarked = serving implies
    s.unsynced.intersect(s.store).subseteq(s.marks.keys())

  /// No view has a generation that a later start-up could give out again.
  val generationsOnlyGrow = and {
    s.generationRecord <= s.generation,
    s.views.forall(v => v.generation <= s.generation),
  }

  val safe = and {
    everyPathHasNarinfo,
    syncedMeansDurable,
    lostDataIsMarked,
    neverServesLostData,
    currentViewsLinkTheStore,
    currentViewsAreClosed,
    collectionSparesPathsInUse,
    boundVolumesHaveViews,
    heldClosuresStay,
    unsyncedPathsAreMarked,
    generationsOnlyGrow,
    s.data.keys() == s.store,
    s.references.keys() == narinfos(s),
  }

  // Witnesses

  val sharesView = VOLUMES.exists(v => VOLUMES.exists(u =>
    v != u and s.bound.has(v) and s.bound.has(u) and s.bound.get(v) == s.bound.get(u)))
  val keepsOldView = s.bound.keys().exists(v => s.bound.get(v).generation < s.generation)
  val holdsLostData = s.store.exists(p => s.data.get(p) == Lost)
  val hasStaleView = s.stale != Set()

  // Actions that both steps share

  action init = s' = {
    store: Set(),
    synced: Set(),
    unsynced: Set(),
    views: Set(),
    volumes: Map(),
    references: Map(),
    generationRecord: 1,
    syncFailed: false,
    bootRecorded: true,
    generation: 1,
    marks: Map(),
    failedSyncs: 0,
    fetches: Map(),
    ensures: Map(),
    sync: RESTING,
    syncs: Set(),
    collection: OFF,
    publishing: Map(),
    unpublishing: Set(),
    startup: UP,
    bound: Map(),
    data: Map(),
    stale: Set(),
    servedLost: false,
    crashes: 0,
    failures: 0,
  }

  action release = {
    nondet e = ENSURES.filter(e => canRelease(s, e)).oneOf()
    s' = released(s, e)
  }

  action fetchBegin = {
    nondet p = PATHS.filter(p => inPhase(s, p, Queued)).oneOf()
    s' = fetchBegun(s, p)
  }

  action fetchFail = {
    nondet p = PATHS.filter(p => canFail(s, p)).oneOf()
    s' = fetchEnded(s, p, false)
  }

  // The small steps

  action ensureResolve = {
    nondet e = ENSURES.filter(e => canResolve(s, e)).oneOf()
    nondet root = PATHS.oneOf()
    nondet choice = CHOICES.oneOf()
    s' = ensureResolved(s, e, root, choice)
  }

  action ensureHold = {
    nondet e = ENSURES.filter(e => canHold(s, e)).oneOf()
    s' = ensureHeld(s, e)
  }

  action ensureJoin = {
    nondet e = ENSURES.filter(e => canJoin(s, e)).oneOf()
    s' = ensureJoined(s, e)
  }

  action fetchMark = {
    nondet p = PATHS.filter(p => inPhase(s, p, Unpacking)).oneOf()
    s' = fetchMarked(s, p)
  }

  action fetchWrite = {
    nondet p = PATHS.filter(p => inPhase(s, p, Marked)).oneOf()
    s' = fetchWritten(s, p)
  }

  action fetchMoveIn = {
    nondet p = PATHS.filter(p => inPhase(s, p, Written)).oneOf()
    s' = fetchMovedIn(s, p)
  }

  action publishStart = {
    nondet v = VOLUMES.oneOf()
    nondet e = ENSURES.filter(e => canPublish(s, v, e)).oneOf()
    s' = publishStarted(s, v, e)
  }

  action publishRecord = {
    nondet v = VOLUMES.filter(v => publishes(s, v, false)).oneOf()
    s' = publishRecorded(s, v)
  }

  action publishBind = {
    nondet v = VOLUMES.filter(v => publishes(s, v, true)).oneOf()
    s' = publishBound(s, v)
  }

  action unpublishStart = {
    nondet v = VOLUMES.filter(v => canUnpublish(s, v)).oneOf()
    s' = unpublishStarted(s, v)
  }

  action unpublishDrop = {
    nondet v = VOLUMES.filter(v => canDrop(s, v)).oneOf()
    s' = unpublishDropped(s, v)
  }

  action syncStart = {
    nondet p = PATHS.filter(p => canStartSync(s, p)).oneOf()
    s' = syncStarted(s, p)
  }

  action syncFlush = {
    nondet ok = Bool.oneOf()
    all { canFlush(s, ok), s' = syncFlushed(s, ok) }
  }

  action syncForget = all { canForget(s), s' = syncForgetBegun(s) }

  action syncForgetPath = {
    nondet p = PATHS.filter(p => canForgetPath(s, p)).oneOf()
    s' = syncForgot(s, p)
  }

  action syncRename = all { canRename(s), s' = syncRenamed(s) }

  /// It may spare any of the paths, as --keep-recent would.
  action collectBegin = {
    nondet doomed = collectable(s).powerset().oneOf()
    all { exclusive(s), s' = collectionBegun(s, doomed) }
  }

  action collectDelete = {
    nondet p = s.collection.doomed.oneOf()
    s' = collectionDeleted(s, p)
  }

  action collectEnd = all { canEndCollection(s), s' = collectionEnded(s) }

  action kill = all { canCrash(s), s' = crashed(s, false) }

  action powerLoss = all { canCrash(s), s' = crashed(s, true) }

  action startScan = {
    nondet flushed = Bool.oneOf()
    all { canScan(s, flushed), s' = startScanned(s, flushed) }
  }

  action startSettle = {
    nondet p = s.startup.todo.oneOf()
    s' = startNarinfoSettled(s, p)
  }

  action startDrop = {
    nondet p = droppable(s).oneOf()
    s' = startDropped(s, p)
  }

  action startNumber = all { canNumber(s), s' = startNumbered(s) }

  action startClear = all {
    s.startup.stage == Clearing,
    s' = startCleared(s),
  }

  action startRecord = all {
    s.startup.stage == Recording,
    s' = startRecorded(s),
  }

  action startPrune = all {
    s.startup.stage == Pruning,
    s' = startPruned(s),
  }

  action step = any {
    ensureResolve, ensureHold, ensureJoin, release,
    fetchBegin, fetchMark, fetchWrite, fetchMoveIn, fetchFail,
    syncStart, syncFlush, syncForget, syncForgetPath, syncRename,
    collectBegin, collectDelete, collectEnd,
    publishStart, publishRecord, publishBind, unpublishStart, unpublishDrop,
    kill, powerLoss,
    startScan, startSettle, startDrop, startNumber, startClear, startRecord,
    startPrune,
  }

  // The whole calls

  action ensure = {
    nondet e = ENSURES.filter(e => canResolve(s, e) and shared(s)).oneOf()
    nondet root = PATHS.oneOf()
    nondet choice = CHOICES.oneOf()
    s' = ensured(s, e, root, choice)
  }

  action fetchEnd = {
    nondet p = PATHS.filter(p => inPhase(s, p, Unpacking)).oneOf()
    s' = fetchInstalled(s, p)
  }

  action sync = {
    nondet ok = Bool.oneOf()
    all { canSync(s, ok), s' = syncedAll(s, ok) }
  }

  action collect = all { exclusive(s), s' = collected(s) }

  action publish = {
    nondet v = VOLUMES.oneOf()
    nondet e = ENSURES.filter(e => canPublish(s, v, e)).oneOf()
    s' = published(s, v, e)
  }

  action unpublish = {
    nondet v = VOLUMES.filter(v => canUnpublish(s, v)).oneOf()
    s' = unpublished(s, v)
  }

  action restart = all { canCrash(s), s' = restarted(s) }

  action reboot = all { canCrash(s), s' = rebooted(s) }

  action replay = any {
    ensure, release,
    fetchBegin, fetchEnd, fetchFail,
    sync, collect,
    publish, unpublish,
    restart, reboot,
  }
}

/// Two ensures that share fetches, with syncs and collection in between,
/// no volume, and two caches that disagree on whether a references b.
module caches {
  import store(
    PATHS = Set("a", "b"),
    REFERENCES = Map("a" -> Set(Set(), Set("b")), "b" -> Set(Set())),
    VOLUMES = Set(),
    ENSURES = Set("e1", "e2"),
    MAX_CRASHES = 0,
    MAX_FAILURES = 0
  ).*
}

/// One path, with two volumes that share its view, a failed syncfs, and
/// two crashes, so that the second can stop the start-up after the first.
module faults {
  import store(
    PATHS = Set("b"),
    REFERENCES = Map("b" -> Set(Set())),
    VOLUMES = Set("v1", "v2"),
    ENSURES = Set("e1", "e2"),
    MAX_CRASHES = 2,
    MAX_FAILURES = 1
  ).*
}

/// For the simulator, and for the traces that the Rust test replays.
module sample {
  import store(
    PATHS = Set("a", "b", "c"),
    REFERENCES = Map("a" -> Set(Set("b")), "b" -> Set(Set()), "c" -> Set(Set("b"))),
    VOLUMES = Set("v1", "v2", "v3"),
    ENSURES = Set("e1", "e2", "e3"),
    MAX_CRASHES = 2,
    MAX_FAILURES = 2
  ).*
}
