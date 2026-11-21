// Scenarios of the model in store.qnt that a random walk rarely takes, each
// with the state it must end in. They also show that the invariants don't
// hold only because some step can never happen. `quint test store_test.qnt`
// runs them.

module store_test {
  import store(
    PATHS = Set("a", "b", "c", "d"),
    REFERENCES = Map(
      "a" -> Set(Set("b")),
      "b" -> Set(Set()),
      "c" -> Set(Set()),
      // Two caches disagree on d.
      "d" -> Set(Set(), Set("c"))
    ),
    VOLUMES = Set("v1", "v2"),
    ENSURES = Set("e1", "e2"),
    MAX_CRASHES = 2,
    MAX_FAILURES = 1
  ).* from "./store"

  /// What one cache gives, and for d the other.
  pure val ONE: Path -> Set[Path] =
    Map("a" -> Set("b"), "b" -> Set(), "c" -> Set(), "d" -> Set())
  pure val OTHER: Path -> Set[Path] = ONE.set("d", Set("c"))

  pure def withEnsure(t: State, e: EnsureId, root: Path): State =
    ensured(t, e, root, ONE)

  pure def withFetch(t: State, p: Path): State =
    fetchInstalled(fetchBegun(t, p), p)

  /// An ensure of a, with its closure fetched.
  pure def withClosure(t: State, e: EnsureId): State =
    withFetch(withFetch(withEnsure(t, e, "a"), "b"), "a")

  /// Volumes with the same root share a view. Collection spares what a
  /// mounted view links or a sync still has to make durable, and deletes it
  /// once the volumes are gone.
  run sharedViewTest =
    init
      .then(s' = withClosure(s, "e1"))
      .then(s' = published(published(s, "v1", "e1"), "v2", "e1"))
      .expect(sharesView and s.views.size() == 1 and safe)
      .then(s' = collected(released(s, "e1"), Set()))
      .expect(s.store == Set("a", "b") and safe)
      .then(s' = collected(unpublished(unpublished(s, "v1"), "v2"), Set()))
      .expect(s.store == Set("a", "b") and s.views == Set() and safe)
      .then(s' = collected(syncedAll(s, true), Set()))
      .expect(s.store == Set() and s.synced == Set() and safe)

  /// An unpublish leaves the view. Collection and a restart may keep it,
  /// with the paths it links, and a later publish shares it. Once a failed
  /// sync starts a new generation, collection deletes it.
  run keptViewTest =
    init
      .then(s' = withClosure(s, "e1"))
      .then(s' = syncedAll(unpublished(published(s, "v1", "e1"), "v1"), true))
      .expect(keepsUnusedView and safe)
      .then(s' = restarted(collected(released(s, "e1"), s.views), s.views))
      .expect(s.store == Set("a", "b") and keepsUnusedView and safe)
      .then(s' = published(withEnsure(s, "e2", "a"), "v2", "e2"))
      .expect(s.publishing == Map() and s.bound.keys() == Set("v2")
        and s.views.size() == 1 and safe)
      .then(s' = unpublished(s, "v2"))
      .then(s' = syncedAll(withFetch(withEnsure(s, "e1", "c"), "c"), false))
      .expect(s.generation == 2 and keepsUnusedView and safe)
      .then(s' = collected(released(released(s, "e1"), "e2"), s.views))
      .expect(s.views == Set() and s.store == Set() and safe)

  /// A failed sync deletes the paths it may have lost and starts a new
  /// generation. The pod keeps its view, a retry of its publish too, and
  /// another volume gets a view of the paths fetched again.
  run failedSyncTest =
    init
      .then(s' = withClosure(s, "e1"))
      .then(s' = published(s, "v1", "e1"))
      .then(s' = syncedAll(s, false))
      .expect(and {
        s.store == Set(),
        s.generation == 2,
        s.syncFailed,
        keepsOldView,
        hasStaleView,
        safe,
      })
      .then(s' = withClosure(s, "e2"))
      .then(s' = published(published(s, "v1", "e2"), "v2", "e2"))
      .expect(and {
        s.bound.get("v1").generation == 1,
        s.bound.get("v2").generation == 2,
        safe,
      })

  /// A failed sync misses a store object that a fetch is still unpacking.
  /// No publish gets it, and its own sync deletes it.
  run lostFetchTest =
    init
      .then(s' = withFetch(withEnsure(s, "e1", "c"), "c"))
      .then(s' = fetchBegun(withEnsure(s, "e2", "b"), "b"))
      .then(s' = fetchInstalled(syncedAll(s, false), "b"))
      .expect(holdsLostData and done(s.ensures.get("e2")) and safe)
      .then(s' = published(s, "v1", "e2"))
      .expect(s.views == Set() and s.bound == Map() and safe)
      .then(s' = syncedAll(s, true))
      .expect(s.store == Set() and s.generation == 3 and safe)

  /// An ensure joins late and queues a fetch of a path that the store has by
  /// then. Collection deletes the path, and the fetch brings it back.
  run refetchTest =
    init
      .then(s' = ensureHeld(ensureResolved(s, "e1", "a", ONE), "e1"))
      .then(s' = ensureHeld(ensureResolved(s, "e2", "a", ONE), "e2"))
      .then(s' = released(released(ensureJoined(s, "e1"), "e1"), "e2"))
      .then(s' = syncedAll(withFetch(s, "b"), true))
      // The fetch of b has ended, so this ensure starts another.
      .then(s' = ensureJoined(s, "e2"))
      .expect(inPhase(s, "b", Queued) and s.store == Set("b"))
      // Their ensures fail, and nothing holds b any more.
      .then(s' = fetchEnded(s, "a", false))
      .expect(s.ensures == Map())
      .then(s' = collectionDeleted(collectionBegun(s, Set(), Set("b")), "b"))
      .expect(s.store == Set() and narinfos(s) == Set())
      .then(s' = collectionEnded(withFetch(s, "b")))
      .expect(s.store == Set("b") and s.unsynced == Set("b") and safe)

  /// Two ensures resolve d from caches that disagree on its references,
  /// and share the fetch that the first one made. The second one's closure
  /// lacks a reference of the store object that came in, so it fails.
  run disagreeingCachesTest =
    init
      .then(s' = withEnsure(ensured(s, "e2", "d", OTHER), "e1", "d"))
      .expect(s.ensures.get("e1").paths == Set("d")
        and s.ensures.get("e2").paths == Set("c", "d"))
      .then(s' = withFetch(withFetch(s, "c"), "d"))
      .expect(and {
        s.references.get("d") == Set("c"),
        s.ensures.get("e1").failed,
        done(s.ensures.get("e2")),
        safe,
      })

  /// An ensure checks its closure of d, a sync fails, and `forget` deletes
  /// d. A fetch that another ensure queued brings d back from the other
  /// cache's narinfo, which references c. The store refuses the first
  /// ensure's publish, since its closure is from before the `forget`.
  run staleClosureTest =
    init
      .then(s' = ensureHeld(ensureResolved(s, "e2", "d", OTHER), "e2"))
      .then(s' = withFetch(withEnsure(s, "e1", "d"), "d"))
      .then(s' = ensureJoined(s, "e2"))
      .expect(done(s.ensures.get("e1")) and inPhase(s, "d", Queued))
      .then(s' = withFetch(syncedAll(s, false), "d"))
      .expect(s.store == Set("d") and not(closed(s, s.ensures.get("e1").paths)))
      .then(s' = published(s, "v1", "e1"))
      .expect(s.views == Set() and s.bound == Map() and safe)

  /// A restart on the same boot syncs and keeps what the last run left
  /// unsynced, and the view that is still mounted.
  run restartTest =
    init
      .then(s' = withClosure(s, "e1"))
      .then(s' = published(s, "v1", "e1"))
      .then(s' = restarted(s, Set()))
      .expect(and {
        s.store == Set("a", "b"),
        s.synced == Set("a", "b"),
        s.unsynced == Set(),
        s.generation == 1,
        s.bound.keys() == Set("v1"),
        s.views.size() == 1,
        safe,
      })

  /// The kernel reports a lost write once. A start-up whose syncfs fails
  /// leaves a record, so if it dies before it has deleted the unsynced
  /// paths, the next start-up deletes them and keeps none as durable.
  run failedStartupSyncTest =
    init
      .then(s' = withFetch(withEnsure(s, "e1", "c"), "c"))
      .then(s' = startScanned(crashed(s, false), false))
      .expect(s.syncFailed and s.data.get("c") == Lost)
      .then(s' = restarted(s, Set()))
      .expect(and {
        s.store == Set(),
        s.synced == Set(),
        s.generation == 2,
        not(s.syncFailed),
        safe,
      })

  /// A reboot keeps only the paths that a sync made durable, and no view,
  /// since it starts a new generation.
  run rebootTest =
    init
      .then(s' = syncedAll(withFetch(withEnsure(s, "e1", "a"), "b"), true))
      .then(s' = published(withFetch(s, "a"), "v1", "e1"))
      .then(s' = rebooted(s, s.views))
      .expect(and {
        s.store == Set("b"),
        s.synced == Set("b"),
        s.generation == 2,
        s.views == Set(),
        s.bound == Map(),
        safe,
      })
}
