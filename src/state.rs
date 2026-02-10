use core::{
    fmt,
    sync::atomic::{AtomicU64, Ordering},
};

#[cfg(feature = "std")]
use std::process::abort;

#[cfg(not(feature = "std"))]
fn abort() -> ! {
    struct PanicOnDrop;

    impl Drop for PanicOnDrop {
        fn drop(&mut self) {
            panic!("aborting")
        }
    }

    let _marker = PanicOnDrop;
    panic!("aborting")
}

const DONE: u64 = 0b0000_0001;
const TAKEN: u64 = 0b0000_0010;
const HANDLE_DROPPED: u64 = 0b0000_0100;
const SCHEDULED: u64 = 0b0000_1000;
const WAKER_LOCK: u64 = 0b0001_0000;
const CANCELLED: u64 = 0b0010_0000;
const YIELDED: u64 = 0b0100_0000;

const REF_COUNT: u64 = 1 << 8;

const INIT: u64 = REF_COUNT;

#[derive(Debug)]
pub struct State(AtomicU64);

impl State {
    pub const fn new() -> State {
        State(AtomicU64::new(INIT))
    }

    pub fn snapshot(&self) -> Snapshot {
        Snapshot(self.0.load(Ordering::Acquire))
    }

    pub fn finish(&self) -> Snapshot {
        Snapshot(self.0.fetch_or(DONE, Ordering::AcqRel))
    }

    pub fn take(&self) -> Snapshot {
        Snapshot(self.0.fetch_or(TAKEN, Ordering::AcqRel))
    }

    pub fn add_ref(&self) -> Snapshot {
        let snapshot = self.0.fetch_add(REF_COUNT, Ordering::AcqRel);
        if snapshot > i64::MAX as u64 {
            // Avoid UB by ensuring the refcount doesn't overflow.
            // We abort here to terminate all threads and to avoid blocking on cleanup.
            // This refcount (over 36 quadrillion) cannot be reached organically, so we don't need to worry about providing a good error message.
            abort();
        }
        Snapshot(snapshot)
    }

    pub fn drop_ref(&self) -> Snapshot {
        Snapshot(self.0.fetch_sub(REF_COUNT, Ordering::AcqRel))
    }

    pub fn drop_handle(&self) -> Snapshot {
        Snapshot(self.0.fetch_or(HANDLE_DROPPED, Ordering::AcqRel))
    }

    pub fn schedule(&self) -> Snapshot {
        Snapshot(self.0.fetch_or(SCHEDULED, Ordering::AcqRel))
    }

    pub fn toggle_waker_lock(&self) -> Snapshot {
        Snapshot(self.0.fetch_xor(WAKER_LOCK, Ordering::AcqRel))
    }

    pub fn run(&self) -> Snapshot {
        Snapshot(self.0.fetch_and(!SCHEDULED & !YIELDED, Ordering::AcqRel))
    }

    pub fn cancel(&self) -> Snapshot {
        Snapshot(self.0.fetch_and(CANCELLED, Ordering::AcqRel))
    }

    pub fn yielded(&self) -> Snapshot {
        Snapshot(self.0.fetch_or(YIELDED, Ordering::AcqRel))
    }
}

#[derive(Clone, Copy)]
pub struct Snapshot(u64);

impl Snapshot {
    pub const fn ref_count(self) -> u64 {
        self.0 >> 8
    }

    pub const fn done(self) -> bool {
        self.0 & DONE > 0
    }

    pub const fn taken(self) -> bool {
        self.0 & TAKEN > 0
    }

    pub const fn handle_dropped(self) -> bool {
        self.0 & HANDLE_DROPPED > 0
    }

    pub const fn scheduled(self) -> bool {
        self.0 & SCHEDULED > 0
    }

    pub const fn waker_lock(self) -> bool {
        self.0 & WAKER_LOCK > 0
    }

    pub const fn cancelled(self) -> bool {
        self.0 & CANCELLED > 0
    }

    pub const fn yielded(self) -> bool {
        self.0 & YIELDED > 0
    }
}

impl fmt::Debug for Snapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Snapshot")
            .field("ref_count", &self.ref_count())
            .field("is_done", &self.done())
            .field("is_taken", &self.taken())
            .field("handle_dropped", &self.handle_dropped())
            .field("scheduled", &self.scheduled())
            .field("waker_lock", &self.waker_lock())
            .field("cancelled", &self.cancelled())
            .field("yielded", &self.yielded())
            .finish()
    }
}
