# atask

atask is a flexible task model for building async executors.

First, create a queue to hold the scheduled tasks.

```rust
let (sender, reciever) = flume::unbounded();
```

A task is spawned using a `spawn*` function.

```rust
let future = async { "hewwo" };

let schedule = move |runnable| sender.send(runnable).unwrap();

let (runnable, handle) = atask::spawn(future);

runnable.schedule();
```

Finally, the spawned tasks must be polled.

```rust
for runnable in receiver {
    runnable.run();
}
```

## Differences from `async-task`

[`async-task`](https://lib.rs/crates/async-task) is a very similar library with a few key differences.

The primary differences are:
- Cancelling or detaching a task does not consume the handle.
- Tasks are detached rather than consumed on dropping the handle.

## Cargo features

- `std` (default): Enables the use of the standard library and enables the `spawn_local` messages.
