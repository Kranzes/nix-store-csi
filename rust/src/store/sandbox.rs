//! Threads that Landlock confines to tmp/, for the code that reads NARs
//! before their hash is checked.

use std::future::Future;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, ensure};
use landlock::{
    ABI, Access, AccessFs, CompatLevel, Compatible, PathBeneath, PathFd, Ruleset, RulesetAttr,
    RulesetCreatedAttr, RulesetStatus,
};
use rustix::fs::{Mode, OFlags, openat};
use rustix::io::Errno;
use tokio::runtime::Runtime;
use tokio_util::task::AbortOnDropHandle;
use tracing::error;

/// A Tokio runtime whose threads can only create directories, files and
/// symlinks in one directory and write to files there. They can't read or
/// write any other file.
pub(super) struct Sandbox(Option<Runtime>);

impl Sandbox {
    /// Starts a worker per CPU, but no more than `jobs`, since each NAR
    /// keeps only a few tasks busy. Fails unless the kernel can enforce the
    /// rules.
    pub fn new(dir: &Path, jobs: usize) -> anyhow::Result<Self> {
        let rules = Arc::new(Rules::new(dir)?);
        let probe = rules.clone();
        std::thread::spawn(move || probe.confine())
            .join()
            .expect("confining a thread does not panic")?;
        let cpus = std::thread::available_parallelism().map_or(1, NonZeroUsize::get);
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(cpus.min(jobs))
            .thread_name("unpack")
            .on_thread_start(move || {
                // A thread that a confined thread spawns inherits the rules.
                if rules.confined() {
                    return;
                }
                // The probe above passed, so this shouldn't fail, and a
                // thread that runs unconfined must not.
                if let Err(e) = rules.confine() {
                    error!("{e:#}");
                    std::process::abort();
                }
            })
            .build()?;
        Ok(Self(Some(runtime)))
    }

    /// Runs `future` on the runtime. Dropping the handle aborts it.
    pub fn spawn<F>(&self, future: F) -> AbortOnDropHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let runtime = self.0.as_ref().expect("the runtime is there until drop");
        AbortOnDropHandle::new(runtime.spawn(future))
    }
}

impl Drop for Sandbox {
    fn drop(&mut self) {
        // A runtime can't block on its shutdown inside another runtime.
        if let Some(runtime) = self.0.take() {
            runtime.shutdown_background();
        }
    }
}

struct Rules {
    dir: PathBuf,
    /// Opened once, since a confined thread can't open it.
    fd: PathFd,
}

impl Rules {
    fn new(dir: &Path) -> anyhow::Result<Self> {
        let fd = PathFd::new(dir)?;
        let dir = dir.to_owned();
        Ok(Self { dir, fd })
    }

    /// Whether the calling thread has the rules, which don't let it list
    /// the directory.
    fn confined(&self) -> bool {
        let flags = OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC;
        openat(&self.fd, ".", flags, Mode::empty()).err() == Some(Errno::ACCESS)
    }

    /// Confines the calling thread, and the threads it spawns later, to the
    /// directory.
    fn confine(&self) -> anyhow::Result<()> {
        let status = Ruleset::default()
            .set_compatibility(CompatLevel::HardRequirement)
            // V3 also handles truncation, the last way to change a file by
            // path.
            .handle_access(AccessFs::from_all(ABI::V3))?
            .create()?
            .add_rule(PathBeneath::new(
                &self.fd,
                AccessFs::MakeDir | AccessFs::MakeReg | AccessFs::MakeSym | AccessFs::WriteFile,
            ))?
            .restrict_self()
            .map_err(anyhow::Error::from)
            .and_then(|status| {
                ensure!(
                    status.ruleset == RulesetStatus::FullyEnforced,
                    "not enforced"
                );
                Ok(status)
            });
        status.map(drop).with_context(|| {
            format!(
                "confining NAR unpacking to {} with Landlock, which needs Linux 6.2 or later \
                 with landlock in its lsm= list",
                self.dir.display()
            )
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::test_utils::scratch;

    #[tokio::test]
    async fn confines_writes_to_its_dir() {
        let tmp = scratch();
        let (inside, outside) = (tmp.path().join("inside"), tmp.path().join("outside"));
        std::fs::create_dir(&inside).unwrap();
        std::fs::create_dir(&outside).unwrap();
        std::fs::write(outside.join("file"), "old").unwrap();
        let sandbox = Sandbox::new(&inside, 2).unwrap();

        let try_writes = |dir: &Path| {
            let dir = dir.to_owned();
            move || -> Vec<std::io::Result<()>> {
                vec![
                    std::fs::create_dir(dir.join("dir")),
                    std::fs::write(dir.join("new"), "data"),
                    std::os::unix::fs::symlink("target", dir.join("link")),
                    std::fs::write(dir.join("file"), "new"),
                ]
            }
        };
        // Harmonia's restore writes from a blocking thread, so both kinds of
        // thread need the rules.
        let (on_worker, writes) = (try_writes(&outside), try_writes(&outside));
        let worker = sandbox.spawn(async move { on_worker() }).await.unwrap();
        let blocking = (sandbox.spawn(async move { tokio::task::spawn_blocking(writes).await }))
            .await
            .unwrap()
            .unwrap();
        for result in worker.into_iter().chain(blocking) {
            let err = result.unwrap_err();
            assert_eq!(err.kind(), std::io::ErrorKind::PermissionDenied, "{err}");
        }
        assert_eq!(std::fs::read(outside.join("file")).unwrap(), b"old");
        assert_eq!(std::fs::read_dir(&outside).unwrap().count(), 1);

        let read = outside.join("file");
        let denied = sandbox.spawn(async move { std::fs::read(read) });
        assert!(denied.await.unwrap().is_err());

        let writes = try_writes(&inside);
        for result in sandbox.spawn(async move { writes() }).await.unwrap() {
            result.unwrap();
        }
        assert_eq!(std::fs::read(inside.join("file")).unwrap(), b"new");
    }

    #[tokio::test]
    async fn starts_no_more_workers_than_jobs() {
        let tmp = scratch();
        let workers = |jobs| {
            let sandbox = Sandbox::new(tmp.path(), jobs).unwrap();
            sandbox.0.as_ref().unwrap().metrics().num_workers()
        };
        assert_eq!(workers(1), 1);
        let cpus = std::thread::available_parallelism().unwrap().get();
        assert_eq!(workers(usize::MAX), cpus);
    }
}
