# Ritualist

Activity scheduling for native apps.

Ritualist is a small, async (Tokio) scheduler for recurring and one-shot
activities. You describe *what* should run and *when* (a fixed interval, a
random interval, or a specific date), and Ritualist hands each due activity to
you over a channel. You run your own logic and acknowledge when you're done —
either completing the activity or snoozing it.

[![Crates.io][crates-badge]][crates-url]
[![MIT licensed][mit-badge]][mit-url]
[![CI][ci-badge]][ci-url]

[crates-badge]: https://img.shields.io/crates/v/ritualist.svg
[crates-url]: https://crates.io/crates/ritualist
[mit-badge]: https://img.shields.io/badge/license-MIT-blue.svg
[mit-url]: https://github.com/yornaath/ritualist/blob/main/LICENSE
[ci-badge]: https://github.com/yornaath/ritualist/actions/workflows/ci.yml/badge.svg
[ci-url]: https://github.com/yornaath/ritualist/actions/workflows/ci.yml

Used by the [Glimt](https://www.glimtapp.io/) app. A breathwork coaching and reminder app for osx.

## Features

- **Interval schedules** — run an activity every fixed `Duration`.
- **Randomized schedules** — run on a random interval within a `[min, max)` range.
- **Date schedules** — run once at a specific `DateTime<Utc>`, then auto-complete.
- **Snooze & done acknowledgements** — each run reports back whether it finished or wants to be re-scheduled later.
- **Pause / resume** — disable and re-enable activities at runtime without losing their place.
- **Snapshots & progress** — inspect live activities and how close each is to firing.
- **Pluggable clock** — swap the system clock for a `TestClock` to drive schedules deterministically in tests.
- **Graceful shutdown** — drain in-flight work, then stop the scheduler.

## Installation

```toml
[dependencies]
ritualist = "0.0.9"
tokio = { version = "1", features = ["macros", "rt-multi-thread", "time", "sync"] }
```

Date-based schedules use `chrono`:

```toml
chrono = "0.4"
```

## Core concepts

| Concept | What it is |
| --- | --- |
| `Ritualist<T>` | The builder/handle you register activities on before running. |
| `RunningRitualist<T>` | The live scheduler returned by `run()`; supports pause, snapshot, and shutdown. |
| `ActivitySpec<T>` | An activity identity (`id: T`) plus its `schedule`. |
| `ActivitySchedule` | `FixedInterval`, `RandomInterval`, or `Scheduled`. |
| `AckMessage` | Your reply after running: `Done` or `Snooze(Duration)`. |

The id type `T` can be anything that is `Debug + Eq + Hash + Copy + Send + 'static`
(an enum is the natural choice). Registering a new spec with an existing id
overwrites the old one and resets its scheduler state.

## Quick start

Schedule two activities at different intervals, then react to each as it fires.
Each due activity arrives on a channel together with an acknowledgement sender;
sending an `AckMessage` back tells Ritualist the run is finished.

```rust
use ritualist::{
    Ritualist,
    ack::AckMessage,
    activity_spec::{ActivitySchedule, ActivitySpec},
};
use std::time::Duration;
use tokio::sync;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Activity {
    Ping,
    Pong,
}

impl Activity {
    fn run(&self, ack: sync::oneshot::Sender<AckMessage>) {
        let activity = *self;
        tokio::spawn(async move {
            match activity {
                Activity::Ping => println!("pinged"),
                Activity::Pong => println!("ponged"),
            }
            // Acknowledge the run so the activity gets re-scheduled.
            let _ = ack.send(AckMessage::Done);
        });
    }
}

#[tokio::main]
async fn main() {
    // buffer = channel capacity, poll_interval = how often the scheduler ticks.
    let mut ritualist = Ritualist::new(64, Duration::from_millis(100));

    ritualist
        .register_many(vec![
            ActivitySpec {
                id: Activity::Ping,
                schedule: ActivitySchedule::FixedInterval {
                    duration: Duration::from_secs(1),
                },
            },
            ActivitySpec {
                id: Activity::Pong,
                schedule: ActivitySchedule::FixedInterval {
                    duration: Duration::from_secs(3),
                },
            },
        ])
        .await
        .expect("invalid activity spec");

    // Take the receiving end *before* starting the scheduler.
    let mut channel = ritualist.take_channel();
    ritualist.run();

    while let Some((activity, ack)) = channel.recv().await {
        activity.run(ack);
    }
}
```

## Schedules

### Fixed interval

Runs every `duration`, resetting after each acknowledged run.

```rust
ActivitySpec {
    id: Activity::Heartbeat,
    schedule: ActivitySchedule::FixedInterval {
        duration: Duration::from_secs(30),
    },
}
```

### Random interval

Picks a fresh target within `[min, max)` for each cycle — handy for jitter so
many activities don't all fire on the same tick. `min` must be strictly less
than `max`, otherwise registration returns `ActivitySpecError::IntervalRangeOverflow`.

```rust
ActivitySpec {
    id: Activity::Sync,
    schedule: ActivitySchedule::RandomInterval {
        min: Duration::from_secs(10),
        max: Duration::from_secs(20),
    },
}
```

### Scheduled (one-shot)

Runs once at a specific wall-clock time, then transitions to `Completed` and is
removed from the scheduler.

```rust
use chrono::{TimeDelta, Utc};

ActivitySpec {
    id: Activity::Reminder,
    schedule: ActivitySchedule::Scheduled {
        date: Utc::now() + TimeDelta::seconds(3),
    },
}
```

## Acknowledging runs: Done vs Snooze

After running an activity you send back an `AckMessage`:

- `AckMessage::Done` — the run succeeded; reschedule per the activity's schedule
  (or complete it if it's a one-shot `Scheduled` activity).
- `AckMessage::Snooze(duration)` — re-run after `duration` instead of the normal
  schedule. Snoozing a one-shot keeps it alive rather than completing it.

```rust
impl Activity {
    fn run(&self, ack: sync::oneshot::Sender<AckMessage>) {
        tokio::spawn(async move {
            if rand::random_bool(1.0 / 3.0) {
                // Not ready yet — try again in 5 seconds.
                let _ = ack.send(AckMessage::Snooze(Duration::from_secs(5)));
            } else {
                println!("pinged");
                let _ = ack.send(AckMessage::Done);
            }
        });
    }
}
```

> If the acknowledgement sender is dropped without sending, Ritualist treats the
> run as `Done`.

## Pausing and resuming

`RunningRitualist::set_enabled` disables or re-enables an activity at runtime.
Disabling preserves the activity's previous state, so re-enabling resumes
exactly where it left off. It's idempotent — disabling twice won't nest.

```rust
let running = ritualist.run();

// ... later, from any task holding the handle:
running.set_enabled(Activity::Ping, false).await; // pause
// ...
running.set_enabled(Activity::Ping, true).await;  // resume
```

## Inspecting live activities

`snapshot()` returns a clone of every tracked `Activity<T>`, including its
current `ActivityState` and how close it is to firing via
`calculate_progress_pct()` (`0.0..=100.0`).

```rust
for activity in running.snapshot().await {
    println!(
        "{:?} [{:?}] {:.0}%",
        activity.spec.id,
        activity.state,
        activity.calculate_progress_pct(),
    );
}
```

`ActivityState` is one of: `Idle`, `Running`, `Snoozed(Duration)`,
`Disabled(prev_state)`, or `Completed`.

## Registering after start

Both `Ritualist` and `RunningRitualist` deref to a shared `RitualistHandle`,
which exposes `register`, `register_many`, `set_enabled`, and `snapshot`, so you
can add activities before or after calling `run()`. The handle is cheap to clone,
so you can hand one out and register activities concurrently while the ritualist
is running.

```rust
let running = ritualist.run();

running
    .register(ActivitySpec {
        id: Activity::Reminder,
        schedule: ActivitySchedule::Scheduled {
            date: Utc::now() + TimeDelta::seconds(2),
        },
    })
    .await
    .expect("invalid activity spec");
```

## Shutting down

`shutdown()` cancels polling, drains in-flight runs (up to a short grace
period), then stops the scheduler. Use `abort()` to stop immediately without
draining, or `join()` to wait for the polling task to finish on its own.

```rust
let running = ritualist.run();
// ... run for a while ...
running.shutdown().await.expect("clean shutdown");
```

## Testing with a custom clock

Ritualist reads time only through the `Clock` trait, so tests can advance time
deterministically with `TestClock` instead of sleeping. Enable the `test-util`
feature to use it outside of this crate's own tests:

```toml
[dev-dependencies]
ritualist = { version = "0.0.9", features = ["test-util"] }
```

```rust
use ritualist::{Ritualist, clock::TestClock};
use chrono::Utc;
use std::{sync::Arc, time::Duration};

let clock = TestClock::new(Utc::now());
let ritualist = Ritualist::<u32>::with_clock(64, Duration::from_millis(10), Arc::new(clock.clone()));

// Drive time forward from the test body; the scheduler observes the same clock.
clock.advance(Duration::from_secs(60));
```

## Examples

Runnable examples live in [`examples/`](examples):

| Example | Demonstrates |
| --- | --- |
| [`ping_pong.rs`](examples/ping_pong.rs) | Two fixed-interval activities running together. |
| [`snoozing.rs`](examples/snoozing.rs) | Replying with `Snooze` to defer a run. |
| [`pausing.rs`](examples/pausing.rs) | Disabling and re-enabling an activity at runtime. |
| [`date_scheduled.rs`](examples/date_scheduled.rs) | One-shot date schedules plus a live progress observer. |

Run one with:

```bash
cargo run --example ping_pong
```

## License

See the repository for license details.
