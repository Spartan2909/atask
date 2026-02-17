use crate::{Error, Result, Schedule, ScheduleInfo, state::State};

use core::{
    cell::UnsafeCell,
    marker::PhantomData,
    mem::{self, ManuallyDrop, MaybeUninit},
    panic::{RefUnwindSafe, UnwindSafe},
    pin::Pin,
    ptr::NonNull,
    task::{self, Context, Poll, RawWaker, RawWakerVTable, Waker},
};

use alloc::boxed::Box;

type PayloadPtr = NonNull<Payload<(), ()>>;
type TaskPtr = NonNull<Task<(), (), (), ()>>;

const fn waker(handle: RawHandle) -> Waker {
    const fn vtable() -> &'static RawWakerVTable {
        &RawWakerVTable::new(clone_waker, wake, wake_by_ref, drop_waker)
    }

    unsafe fn clone_waker(ptr: *const ()) -> RawWaker {
        // SAFETY: `ptr` must be a valid handle.
        let handle: RawHandle = unsafe { RawHandle::clone_from_raw(ptr) };
        RawWaker::new(handle.into_raw(), vtable())
    }

    fn wake_inner(handle: RawHandle) {
        if !handle.header().state.schedule().scheduled() {
            // SAFETY: `handle` is not currently scheduled.
            unsafe {
                (handle.header().vtable().schedule)(handle);
            }
        }
    }

    unsafe fn wake(ptr: *const ()) {
        // SAFETY: `ptr` is a valid raw handle.
        let handle: RawHandle = unsafe { RawHandle::from_raw(ptr) };
        wake_inner(handle);
    }

    unsafe fn wake_by_ref(ptr: *const ()) {
        // SAFETY: `ptr` is a valid raw handle.
        let handle: RawHandle = unsafe { RawHandle::clone_from_raw(ptr) };
        wake_inner(handle);
    }

    unsafe fn drop_waker(ptr: *const ()) {
        // SAFETY: `ptr` is a valid raw handle.
        let handle: RawHandle = unsafe { RawHandle::from_raw(ptr) };
        drop(handle);
    }

    let raw = RawWaker::new(handle.into_raw(), vtable());
    // SAFETY: `raw` is a valid waker.
    unsafe { Waker::from_raw(raw) }
}

struct VTable {
    /// # Safety
    ///
    /// The caller must have exclusive access to the payload.
    poll: unsafe fn(PayloadPtr, &mut Context<'_>) -> Poll<()>,
    /// # Safety
    ///
    /// The caller must have exclusive access to the payload.
    drop_future: unsafe fn(PayloadPtr),
    /// # Safety
    ///
    /// The caller must have exclusive access to the payload.
    drop_value: unsafe fn(PayloadPtr),
    /// # Safety
    ///
    /// The caller must have exclusive access to the payload.
    set_error: unsafe fn(&RawHandle, error: Error),
    /// # Safety
    ///
    /// The caller must have exclusive access to the payload and the return value must be set.
    take_output: unsafe fn(&RawHandle, out: *mut ()),
    /// # Safety
    ///
    /// The caller must have exclusive access to the task.
    dealloc: unsafe fn(TaskPtr),
    /// # Safety
    ///
    /// The task must not be currently scheduled.
    schedule: unsafe fn(RawHandle),
    metadata: fn(&RawHandle) -> NonNull<()>,
    new_waker: fn(RawHandle) -> Waker,
}

impl VTable {
    const fn of<'a, F, R, S, M>() -> &'a VTable
    where
        F: Future<Output = Result<R>>,
        S: Schedule<M>,
    {
        unsafe fn poll<F: Future<Output = Result<R>>, R>(
            payload: NonNull<Payload<(), ()>>,
            cx: &mut Context<'_>,
        ) -> Poll<()> {
            let mut payload: NonNull<Payload<F, R>> = payload.cast();
            // SAFETY: `payload` must be valid.
            let payload = unsafe { payload.as_mut() };
            // SAFETY: `payload` must be pollable.
            let fut = unsafe { &mut payload.future };
            // SAFETY: `payload` is pinned.
            let fut = unsafe { Pin::new_unchecked(&mut **fut) };
            let res = task::ready!(fut.poll(cx));
            payload.value = ManuallyDrop::new(res);
            Poll::Ready(())
        }

        unsafe fn drop_in_place<T>(ptr: NonNull<Payload<(), ()>>) {
            let ptr: NonNull<T> = ptr.cast();
            // SAFETY: Must be ensured by caller.
            unsafe {
                ptr.drop_in_place();
            }
        }

        const unsafe fn set_error<R>(task: &RawHandle, error: Error) {
            let payload: NonNull<Result<R>> = task.payload().cast();
            // SAFETY: We must have write access to the payload.
            unsafe {
                payload.write(Err(error));
            }
        }

        const unsafe fn take_output<R>(task: &RawHandle, out: *mut ()) {
            let out: *mut Result<R> = out.cast();
            let value: *const Result<R> = task.payload().as_ptr().cast();
            // SAFETY: Must be ensured by caller.
            unsafe {
                value.copy_to_nonoverlapping(out, 1);
            }
        }

        unsafe fn dealloc<F, R, S, M>(ptr: NonNull<Task<(), (), (), ()>>) {
            let ptr: NonNull<Task<F, R, S, M>> = ptr.cast();
            // SAFETY: `ptr` must be valid for this.
            let task = unsafe { Box::from_raw(ptr.as_ptr()) };
            drop(task);
        }

        unsafe fn schedule<F, R, S: Schedule<M>, M>(task: RawHandle) {
            let ptr: *mut Task<F, R, S, M> = task.0.as_ptr().cast();
            // SAFETY: `task` must be a valid task.
            let scheduler = unsafe { &(*ptr).scheduler };
            let schedule_info = ScheduleInfo {
                woken_while_running: task.header().state.snapshot().yielded(),
            };
            // SAFETY: Must be ensured by caller.
            scheduler.schedule(unsafe { Runnable::new(task) }, schedule_info);
        }

        fn metadata<F, R, S, M>(task: &RawHandle) -> NonNull<()> {
            let ptr: NonNull<Task<F, R, S, M>> = task.0.cast();
            // SAFETY: The metadata is always valid to access.
            let metadata = unsafe { &(*ptr.as_ptr()).metadata };
            NonNull::from_ref(metadata).cast()
        }

        &VTable {
            poll: poll::<F, R>,
            drop_future: drop_in_place::<F>,
            drop_value: drop_in_place::<R>,
            set_error: set_error::<R>,
            take_output: take_output::<R>,
            dealloc: dealloc::<F, R, S, M>,
            schedule: schedule::<F, R, S, M>,
            metadata: metadata::<F, R, S, M>,
            new_waker: waker,
        }
    }
}

struct Header {
    state: State,
    vtable: NonNull<VTable>,
    waker: UnsafeCell<Option<Waker>>,
}

impl Header {
    fn vtable<'a>(&self) -> &'a VTable {
        // SAFETY: `self.vtable` is a static reference.
        unsafe { &*self.vtable.as_ptr() }
    }
}

#[repr(C)]
union Payload<F, R> {
    _empty: (),
    future: ManuallyDrop<F>,
    value: ManuallyDrop<Result<R>>,
}

#[cfg_attr(
    any(
        target_arch = "x86_64",
        target_arch = "aarch64",
        target_arch = "powerpc64",
    ),
    repr(align(128))
)]
#[cfg_attr(
    any(
        target_arch = "arm",
        target_arch = "mips",
        target_arch = "mips64",
        target_arch = "sparc",
        target_arch = "hexagon",
    ),
    repr(align(32))
)]
#[cfg_attr(target_arch = "m68k", repr(align(16)))]
#[cfg_attr(target_arch = "s390x", repr(align(256)))]
#[cfg_attr(
    not(any(
        target_arch = "x86_64",
        target_arch = "aarch64",
        target_arch = "powerpc64",
        target_arch = "arm",
        target_arch = "mips",
        target_arch = "mips64",
        target_arch = "sparc",
        target_arch = "hexagon",
        target_arch = "m68k",
        target_arch = "s390x",
    )),
    repr(align(64))
)]
#[repr(C)]
pub struct Task<F, R, S, M> {
    header: Header,
    payload: Payload<F, R>,
    scheduler: S,
    metadata: M,
}

impl<F, R, S, M> Task<F, R, S, M>
where
    F: Future<Output = Result<R>>,
    S: Schedule<M>,
{
    pub fn allocate(future: F, scheduler: S, metadata: M) -> (Runnable<M>, RawJoinHandle<R, M>) {
        let task = Box::new(Task {
            header: Header {
                state: State::new(),
                vtable: NonNull::from_ref(VTable::of::<F, R, S, M>()),
                waker: UnsafeCell::new(None),
            },
            payload: Payload::<F, R> {
                future: ManuallyDrop::new(future),
            },
            scheduler,
            metadata,
        });
        let ptr = NonNull::from_mut(Box::leak(task));
        let handle = RawHandle(ptr.cast());
        // SAFETY: This is the only join handle.
        let join_handle = unsafe { RawJoinHandle::new(handle.clone()) };
        // SAFETY: This is the only runnable.
        let runnable = unsafe { Runnable::new(handle) };
        (runnable, join_handle)
    }
}

pub struct RawHandle(NonNull<Header>);

impl RawHandle {
    const fn header(&self) -> &Header {
        // SAFETY: `self` is a valid pointer to a task, and the header is in the correct place.
        unsafe { self.0.as_ref() }
    }

    const fn payload(&self) -> NonNull<Payload<(), ()>> {
        let ptr: NonNull<Task<(), (), (), ()>> = self.0.cast();
        // SAFETY: `self` is a valid pointer to a task.
        let ptr = unsafe { &raw mut (*ptr.as_ptr()).payload };
        // SAFETY: `ptr` is valid.
        unsafe { NonNull::new_unchecked(ptr) }
    }

    const fn into_raw(self) -> *const () {
        let ptr = self.0.as_ptr().cast();
        mem::forget(self);
        ptr
    }

    const unsafe fn from_raw(ptr: *const ()) -> RawHandle {
        // SAFETY: Must be ensured by caller.
        let ptr: NonNull<Header> = unsafe { NonNull::new_unchecked(ptr.cast_mut().cast()) };
        RawHandle(ptr)
    }

    unsafe fn clone_from_raw(ptr: *const ()) -> RawHandle {
        // SAFETY: `ptr` must be a valid handle.
        let handle = unsafe { RawHandle::from_raw(ptr) };
        handle.header().state.add_ref();
        handle
    }

    pub fn cancel(&self) {
        self.header().state.cancel();
    }
}

impl Clone for RawHandle {
    fn clone(&self) -> Self {
        self.header().state.add_ref();
        RawHandle(self.0)
    }
}

impl UnwindSafe for RawHandle {}

impl RefUnwindSafe for RawHandle {}

impl Drop for RawHandle {
    fn drop(&mut self) {
        let snapshot = self.header().state.drop_ref();
        if snapshot.ref_count() > 1 {
            return;
        }

        if snapshot.done() {
            if !snapshot.taken() {
                // SAFETY: The last ref has been dropped and the value remains.
                unsafe {
                    (self.header().vtable().drop_value)(self.payload());
                }
            }
        } else {
            // SAFETY: The last ref has been dropped and the future remains.
            unsafe {
                (self.header().vtable().drop_future)(self.payload());
            }
        }

        // SAFETY: The last ref has been dropped.
        unsafe {
            (self.header().vtable().dealloc)(self.0.cast());
        }
    }
}

/// A task which can be run or scheduled.
pub struct Runnable<M = ()> {
    handle: RawHandle,
    _marker: PhantomData<M>,
}

impl<M> Runnable<M> {
    const unsafe fn new(handle: RawHandle) -> Runnable<M> {
        Runnable {
            handle,
            _marker: PhantomData,
        }
    }

    /// Pass this task to the scheduler.
    #[inline]
    pub fn schedule(self) {
        if !self.handle.header().state.schedule().scheduled() {
            // SAFETY: `self.0` is not currently scheduled.
            unsafe {
                (self.handle.header().vtable().schedule)(self.handle);
            }
        }
    }

    fn wake_waiter(&self) {
        let snapshot = self.handle.header().state.finish();
        if !snapshot.waker_lock() {
            // SAFETY: The waker lock is not currently held, and will not be taken while state is ready.
            let waker = unsafe { &*self.handle.header().waker.get() };
            if let Some(waker) = waker {
                waker.wake_by_ref();
            }
        }
    }

    /// Run this task to the next yield point.
    #[inline]
    pub fn run(self) {
        if self.handle.header().state.snapshot().cancelled() {
            // SAFETY: We have exclusive access to the payload.
            unsafe { (self.handle.header().vtable().set_error)(&self.handle, Error::Cancelled) }
            self.handle.header().state.finish();
            self.wake_waiter();
            return;
        }

        let waker = (self.handle.header().vtable().new_waker)(self.handle.clone());
        let mut cx = Context::from_waker(&waker);
        self.handle.header().state.run();
        // SAFETY: `self.0.payload()` is a valid payload.
        let poll = unsafe { (self.handle.header().vtable().poll)(self.handle.payload(), &mut cx) };
        if self.handle.header().state.snapshot().yielded() {
            self.handle.header().state.yielded();
        }

        if poll.is_ready() {
            self.wake_waiter();
        }
    }

    /// The metadata associated with this task.
    pub fn metadata(&self) -> &M {
        let ptr = (self.handle.header().vtable().metadata)(&self.handle);
        // SAFETY: This is the correct metadata type.
        unsafe { &*ptr.cast().as_ptr() }
    }
}

// SAFETY: Must be ensured on construction.
unsafe impl<M: Sync> Send for Runnable<M> {}

// SAFETY: Must be ensured on construction.
unsafe impl<M: Sync> Sync for Runnable<M> {}

pub struct RawJoinHandle<T, M> {
    handle: RawHandle,
    _marker: PhantomData<(fn() -> T, M)>,
}

impl<T, M> RawJoinHandle<T, M> {
    unsafe fn new(handle: RawHandle) -> RawJoinHandle<T, M> {
        RawJoinHandle {
            handle,
            _marker: PhantomData,
        }
    }

    pub fn metadata(&self) -> &M {
        let ptr = (self.handle.header().vtable().metadata)(&self.handle);
        // SAFETY: This is the correct metadata type.
        unsafe { &*ptr.cast().as_ptr() }
    }

    fn try_take_output(&self) -> Poll<Result<T>> {
        let snapshot = self.handle.header().state.toggle_waker_lock();
        if snapshot.done() {
            assert!(
                !self.handle.header().state.take().taken(),
                "value taken twice"
            );
            let mut out: MaybeUninit<Result<T>> = MaybeUninit::uninit();
            // SAFETY: We know the output is present, and we are the only one allowed to take it.
            unsafe {
                (self.handle.header().vtable().take_output)(&self.handle, out.as_mut_ptr().cast());
            }
            // SAFETY: `out` has been initialised by the call to `take_output`.
            unsafe { Poll::Ready(out.assume_init()) }
        } else {
            Poll::Pending
        }
    }

    pub fn poll(&self, cx: &Context<'_>) -> Poll<Result<T>> {
        if let Poll::Ready(value) = self.try_take_output() {
            return Poll::Ready(value);
        }

        // SAFETY: We hold the waker lock.
        let waker = unsafe { &mut *self.handle.header().waker.get() };
        *waker = Some(cx.waker().clone());

        self.try_take_output()
    }

    pub fn cancel(&self) {
        self.handle.cancel();
    }

    pub const fn handle(&self) -> &RawHandle {
        &self.handle
    }

    pub fn finished(&self) -> bool {
        self.handle.header().state.snapshot().done()
    }
}

impl<T, M> Unpin for RawJoinHandle<T, M> {}

impl<T, M> Drop for RawJoinHandle<T, M> {
    fn drop(&mut self) {
        self.handle.header().state.drop_handle();
    }
}
