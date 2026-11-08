// SPDX-License-Identifier: MPL-2.0

use core::{
    borrow::Borrow,
    fmt::Display,
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};

use keyable_arc::KeyableWeak;

use super::{
    EpollCtl, EpollEvent, EpollFlags,
    entry::{Entry, EntryKey, ReadySet},
};
use crate::{
    events::IoEvents,
    fs::{
        file::{
            AccessMode, CreationFlags, FileCommon, FileLike, StatusFlags,
            file_table::{FdFlags, FileDesc, get_file_fast},
        },
        pseudofs::AnonInodeFs,
    },
    prelude::*,
    process::{
        posix_thread::ThreadLocal,
        signal::{PollHandle, Pollable},
    },
    util::ioctl::RawIoctl,
};

/// Serializes adding an epoll file to another epoll file.
///
/// Every `EPOLL_CTL_ADD` command whose target is an epoll file holds this lock
/// from the topology check until the insertion into the interest list
/// completes. Without it, two concurrent commands adding two epoll files to
/// each other could both pass the check before either of them inserts, and
/// together create a cycle.
static EPOLL_TOPOLOGY_LOCK: Mutex<()> = Mutex::new(());

/// The maximum number of nested epoll files.
///
/// Watching an epoll file through more than this many levels of other epoll
/// files is rejected with `ELOOP`, as on Linux, which applies the same limit
/// of four.
///
/// Reference: <https://elixir.bootlin.com/linux/v6.18/source/fs/eventpoll.c#L94>.
const MAX_EPOLL_NESTING: usize = 4;

/// A file-like object that provides epoll API.
///
/// Conceptually, we maintain two lists: one consists of all interesting files,
/// which can be managed by the epoll ctl commands; the other are for ready files,
/// which are files that have some events. A epoll wait only needs to iterate the
/// ready list and poll each file to see if the file is ready for the interesting
/// I/O.
///
/// To maintain the ready list, we need to monitor interesting events that happen
/// on the files. To do so, the `EpollFile` registers itself as an `Observer` to
/// the monotored files. Thus, we can add a file to the ready list when an interesting
/// event happens on the file.
pub(crate) struct EpollFile {
    // All interesting entries.
    interest: Mutex<BTreeSet<EntryHolder>>,
    // A set of ready entries.
    //
    // Keep this in a separate `Arc` to avoid dropping `EpollFile` in the observer callback, which
    // may cause deadlocks.
    ready: Arc<ReadySet>,
    /// A lower bound of the number of epoll files that watch this epoll file,
    /// directly or through other epoll files, along the longest chain.
    ///
    /// This field is only accessed while holding `EPOLL_TOPOLOGY_LOCK`, and it
    /// never decreases: removing an entry with `EPOLL_CTL_DEL` does not lower
    /// it, so it may overestimate the current topology.
    up_watch_depth: AtomicUsize,
    /// The common state for this epoll file.
    common: FileCommon,
}

impl EpollFile {
    /// Creates a new epoll file.
    pub(crate) fn new() -> Arc<Self> {
        let pseudo_path = AnonInodeFs::new_path(|_| "anon_inode:[eventpoll]".to_string());

        Arc::new(Self {
            interest: Mutex::new(BTreeSet::new()),
            ready: Arc::new(ReadySet::new()),
            up_watch_depth: AtomicUsize::new(0),
            // Reference: <https://elixir.bootlin.com/linux/v7.0/source/fs/eventpoll.c#L2191>.
            common: FileCommon::new(pseudo_path, AccessMode::O_RDWR, StatusFlags::empty()),
        })
    }

    /// Controls the interest list of the epoll file.
    pub(crate) fn control(&self, thread_local: &ThreadLocal, cmd: &EpollCtl) -> Result<()> {
        let fd = match cmd {
            EpollCtl::Add(fd, ..) => *fd,
            EpollCtl::Del(fd) => *fd,
            EpollCtl::Mod(fd, ..) => *fd,
        };

        let mut file_table = thread_local.borrow_file_table_mut();
        let file = get_file_fast!(&mut file_table, fd).into_owned();
        drop(file_table);

        match *cmd {
            EpollCtl::Add(fd, ep_event, ep_flags) => {
                if let Some(target) = file.downcast_ref::<EpollFile>() {
                    self.add_epoll_interest(fd, &file, ep_event, ep_flags, target)
                } else {
                    self.add_interest(fd, file, ep_event, ep_flags)
                }
            }
            EpollCtl::Del(fd) => self.del_interest(fd, Arc::downgrade(&file).into()),
            EpollCtl::Mod(fd, ep_event, ep_flags) => {
                self.mod_interest(fd, file, ep_event, ep_flags)
            }
        }
    }

    /// Adds an epoll file to the interest list of `self`, after checking that
    /// the topology of epoll files stays acyclic and shallow.
    ///
    /// Like on Linux, adding an epoll file to itself fails with `EINVAL`, and
    /// adding it so that the epoll files form a cycle, or so that an epoll
    /// file watches another through more than `MAX_EPOLL_NESTING` levels of
    /// epoll files, fails with `ELOOP`. Beyond the semantics, a cycle would
    /// deadlock the system: when a watched file becomes ready, the
    /// notification travels through the chain of epoll files synchronously,
    /// so a cycle leads an epoll file's notification to re-enter its own
    /// ready list, which is protected by a spin lock.
    fn add_epoll_interest(
        &self,
        fd: FileDesc,
        file: &Arc<dyn FileLike>,
        ep_event: EpollEvent,
        ep_flags: EpollFlags,
        target: &EpollFile,
    ) -> Result<()> {
        if core::ptr::eq(self, target) {
            return_errno_with_message!(Errno::EINVAL, "cannot add an epoll file to itself");
        }

        // The lock is held until the insertion (and the bookkeeping below)
        // completes, so that the check and the insertion appear atomic.
        let _topology = EPOLL_TOPOLOGY_LOCK.lock();

        self.check_epoll_topology(file)?;

        let result = self.add_interest(fd, file.clone(), ep_event, ep_flags);
        if result.is_ok() {
            self.update_up_watch_depth(file);
        }
        result
    }

    /// Checks that adding `target` to the interest list of `self` does not
    /// close a cycle of epoll files and does not make the nesting of epoll
    /// files deeper than `MAX_EPOLL_NESTING`.
    ///
    /// This method must be called while holding `EPOLL_TOPOLOGY_LOCK`.
    fn check_epoll_topology(&self, target: &Arc<dyn FileLike>) -> Result<()> {
        let mut scanned = BTreeMap::new();
        let (watches_self, watch_depth) = self.scan_epoll_watches(target, &mut scanned);

        if watches_self {
            return_errno_with_message!(Errno::ELOOP, "adding this epoll file would create a cycle");
        }

        let up_watch_depth = self.up_watch_depth.load(Ordering::Relaxed);
        if up_watch_depth + 1 + watch_depth > MAX_EPOLL_NESTING {
            return_errno_with_message!(
                Errno::ELOOP,
                "adding this epoll file would nest epoll files too deeply"
            );
        }

        Ok(())
    }

    /// Scans the epoll files watched by `target`, directly or through other
    /// epoll files.
    ///
    /// Returns whether `self` is among them, and the number of epoll files
    /// between `target` and the most deeply nested epoll file below it.
    /// Results for scanned files are memoized in `scanned`, which also keeps
    /// the files alive so that their addresses, used as keys, are not reused
    /// while the scan is in progress.
    ///
    /// The recursion depth is bounded by `MAX_EPOLL_NESTING`: the topology
    /// check rejects any addition that would make a chain of watches longer
    /// than it, so all chains of watches are within the limit.
    fn scan_epoll_watches(
        &self,
        target: &Arc<dyn FileLike>,
        scanned: &mut BTreeMap<*const EpollFile, ScannedEpoll>,
    ) -> (bool, usize) {
        let target_epoll = target.downcast_ref::<EpollFile>().unwrap();

        let mut result = ScannedEpoll {
            file: target.clone(),
            watches_self: core::ptr::eq(target_epoll, self),
            watch_depth: 0,
        };

        for file in target_epoll.epoll_interests() {
            let Some(epoll_file) = file.downcast_ref::<EpollFile>() else {
                continue;
            };
            let key = epoll_file as *const EpollFile;
            let sub = if let Some(result) = scanned.get(&key) {
                (result.watches_self, result.watch_depth)
            } else {
                self.scan_epoll_watches(&file, scanned)
            };
            result.watches_self |= sub.0;
            result.watch_depth = result.watch_depth.max(sub.1 + 1);
        }

        let ret = (result.watches_self, result.watch_depth);
        scanned.insert(target_epoll as *const EpollFile, result);
        ret
    }

    /// Raises the `up_watch_depth` of `target` and of the epoll files below
    /// it after `target` has been added to the interest list of `self`.
    ///
    /// This method must be called while holding `EPOLL_TOPOLOGY_LOCK`, after
    /// the insertion has succeeded.
    fn update_up_watch_depth(&self, target: &Arc<dyn FileLike>) {
        let depth = self.up_watch_depth.load(Ordering::Relaxed) + 1;
        let mut to_scan = vec![(target.clone(), depth)];

        while let Some((file, depth)) = to_scan.pop() {
            let Some(epoll_file) = file.downcast_ref::<EpollFile>() else {
                continue;
            };

            // An already sufficient depth implies that the files below have
            // also reached sufficient depths when it was set.
            if epoll_file.up_watch_depth.load(Ordering::Relaxed) >= depth {
                continue;
            }
            epoll_file.up_watch_depth.store(depth, Ordering::Relaxed);

            for file in epoll_file.epoll_interests() {
                if file.downcast_ref::<EpollFile>().is_some() {
                    to_scan.push((file, depth + 1));
                }
            }
        }
    }

    /// Returns the epoll files in the interest list of `self`.
    ///
    /// Entries whose files have been dropped are ignored.
    fn epoll_interests(&self) -> Vec<Arc<dyn FileLike>> {
        self.interest
            .lock()
            .iter()
            .filter_map(|holder| holder.0.file())
            .filter(|file| file.downcast_ref::<EpollFile>().is_some())
            .collect()
    }

    fn add_interest(
        &self,
        fd: FileDesc,
        file: Arc<dyn FileLike>,
        ep_event: EpollEvent,
        ep_flags: EpollFlags,
    ) -> Result<()> {
        self.warn_unsupported_flags(&ep_flags);

        // Add the new entry to the interest list and start monitoring its events
        let ready_entry = {
            let mut interest = self.interest.lock();

            if interest.contains(&EntryKey::from((fd, &file))) {
                return_errno_with_message!(
                    Errno::EEXIST,
                    "the file is already in the interest list"
                );
            }

            let entry = Entry::new(fd, Arc::downgrade(&file).into(), self.ready.clone());
            let events = entry.update(ep_event, ep_flags);

            let ready_entry = if !events.is_empty() {
                Some(entry.clone())
            } else {
                None
            };

            let inserted = interest.insert(entry.into());
            assert!(inserted);

            ready_entry
        };

        // Add the new entry to the ready list if the file is ready
        if let Some(entry) = ready_entry {
            self.ready.push(entry.observer());
        }

        Ok(())
    }

    fn del_interest(&self, fd: FileDesc, file: KeyableWeak<dyn FileLike>) -> Result<()> {
        // If this epoll entry is in the ready list, then we should delete it.
        // But unfortunately, deleting an entry from the ready list has a
        // complexity of O(N).
        //
        // To optimize performance, we postpone the actual deletion to the time
        // when the ready list is scanned in `EpolFile::wait`. This can be done
        // because the strong reference count will reach zero and `Weak::upgrade`
        // will fail.

        if !self.interest.lock().remove(&EntryKey::from((fd, file))) {
            return_errno_with_message!(Errno::ENOENT, "the file is not in the interest list");
        }

        Ok(())
    }

    fn mod_interest(
        &self,
        fd: FileDesc,
        file: Arc<dyn FileLike>,
        new_ep_event: EpollEvent,
        new_ep_flags: EpollFlags,
    ) -> Result<()> {
        self.warn_unsupported_flags(&new_ep_flags);

        // Update the epoll entry
        let ready_entry = {
            let interest = self.interest.lock();

            let EntryHolder(entry) =
                interest.get(&EntryKey::from((fd, &file))).ok_or_else(|| {
                    Error::with_message(Errno::ENOENT, "the file is not in the interest list")
                })?;
            let events = entry.update(new_ep_event, new_ep_flags);

            if !events.is_empty() {
                Some(entry.clone())
            } else {
                None
            }
        };

        // Add the updated entry to the ready list if the file is ready
        if let Some(entry) = ready_entry {
            self.ready.push(entry.observer());
        }

        Ok(())
    }

    /// Waits for interesting events happen on the files in the interest list
    /// of the epoll file.
    ///
    /// This method blocks until either some interesting events happen or
    /// the timeout expires or a signal arrives. The first case returns
    /// `Ok(events)`, where `events` is a `Vec` containing at most `max_events`
    /// number of `EpollEvent`s. The second and third case returns errors.
    ///
    /// When `max_events` equals to zero, the method returns when the timeout
    /// expires or a signal arrives.
    pub(crate) fn wait(
        &self,
        max_events: usize,
        timeout: Option<&Duration>,
    ) -> Result<Vec<EpollEvent>> {
        let mut ep_events = Vec::new();

        self.wait_events(IoEvents::IN, timeout, || {
            self.pop_multi_ready(max_events, &mut ep_events);

            if ep_events.is_empty() {
                return Err(Error::with_message(
                    Errno::EAGAIN,
                    "there are no available events",
                ));
            }

            Ok(())
        })?;

        Ok(ep_events)
    }

    fn pop_multi_ready(&self, max_events: usize, ep_events: &mut Vec<EpollEvent>) {
        let mut pop_iter = self.ready.lock_pop();

        loop {
            if ep_events.len() >= max_events {
                break;
            }

            // Since we're holding `pop_guard` (in `pop_iter`), no one else can pop the entries
            // from the ready list. This guarantees that `next` will pop the ready entries we see
            // when `pop_multi_ready` starts executing, so that such entries are never duplicated.
            let Some(entry) = pop_iter.next() else {
                break;
            };

            // Poll the events. If the file is dead, we will remove the entry.
            let Some((ep_event, is_still_ready)) = entry.poll() else {
                // We're removing entries whose files are dead. This can only fail if user programs
                // remove the entry at the same time, and we run into some race conditions.
                //
                // However, this has very limited impact because we will never remove a wrong entry. So
                // the error can be silently ignored.
                let _ = self.del_interest(entry.fd(), entry.file_weak().clone());
                continue;
            };

            // Save the event in the output vector, if any.
            if let Some(event) = ep_event {
                ep_events.push(event);
            }

            // Add the entry back to the ready list, if necessary.
            if is_still_ready {
                self.ready.push(entry.observer());
            }
        }
    }

    fn warn_unsupported_flags(&self, flags: &EpollFlags) {
        if flags.intersects(EpollFlags::EXCLUSIVE | EpollFlags::WAKE_UP) {
            warn!("{:?} contains unsupported flags", flags);
        }
    }
}

impl Pollable for EpollFile {
    fn poll(&self, mask: IoEvents, poller: Option<&mut PollHandle>) -> IoEvents {
        self.ready.poll(mask, poller)
    }
}

// Implement the common methods required by FileHandle
impl FileLike for EpollFile {
    fn ioctl(&self, _raw_ioctl: RawIoctl) -> Result<i32> {
        return_errno_with_message!(Errno::ENOTTY, "epoll files do not support ioctl");
    }

    fn common(&self) -> &FileCommon {
        &self.common
    }

    fn dump_proc_fdinfo(self: Arc<Self>, fd_flags: FdFlags) -> Box<dyn Display> {
        struct FdInfo {
            inner: Arc<EpollFile>,
            fd_flags: FdFlags,
        }

        impl Display for FdInfo {
            fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
                let mut flags = self.inner.common.status_flags().bits()
                    | self.inner.common.access_mode() as u32;
                if self.fd_flags.contains(FdFlags::CLOEXEC) {
                    flags |= CreationFlags::O_CLOEXEC.bits();
                }

                writeln!(f, "pos:\t{}", 0)?;
                writeln!(f, "flags:\t0{:o}", flags)?;
                writeln!(f, "mnt_id:\t{}", AnonInodeFs::mount_node().id())?;
                writeln!(f, "ino:\t{}", AnonInodeFs::shared_inode().ino())?;
                for entry in self.inner.interest.lock().iter() {
                    writeln!(f, "{}", entry.0)?;
                }

                Ok(())
            }
        }

        Box::new(FdInfo {
            inner: self,
            fd_flags,
        })
    }
}

/// The memoized result of scanning an epoll file in
/// [`EpollFile::scan_epoll_watches`].
struct ScannedEpoll {
    // The scanned epoll file, kept alive so that its address, which keys the
    // memoized results, is not reused while the scan is in progress.
    #[expect(dead_code)]
    file: Arc<dyn FileLike>,
    // Whether the scanned epoll file watches, directly or through other epoll
    // files, the epoll file being added to.
    watches_self: bool,
    // The number of epoll files between the scanned epoll file and the most
    // deeply nested epoll file below it.
    watch_depth: usize,
}

struct EntryHolder(Arc<Entry>);

impl PartialOrd for EntryHolder {
    fn partial_cmp(&self, other: &Self) -> Option<core::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for EntryHolder {
    fn cmp(&self, other: &Self) -> core::cmp::Ordering {
        self.0.key().cmp(other.0.key())
    }
}
impl PartialEq for EntryHolder {
    fn eq(&self, other: &Self) -> bool {
        self.0.key().eq(other.0.key())
    }
}
impl Eq for EntryHolder {}

impl Borrow<EntryKey> for EntryHolder {
    fn borrow(&self) -> &EntryKey {
        self.0.key()
    }
}

impl From<Arc<Entry>> for EntryHolder {
    fn from(value: Arc<Entry>) -> Self {
        Self(value)
    }
}

impl Drop for EntryHolder {
    fn drop(&mut self) {
        self.0.shutdown();
    }
}
