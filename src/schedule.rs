use crate::{
    ack::{Ack, AckMessage},
    activity::{Activity, ActivityId, ActivityState},
    activity_spec::ActivitySpec,
    clock::Clock,
};
use std::{collections::HashMap, fmt::Debug, sync::Arc, time::Duration};
use chrono::Duration;
use tokio::{
    select,
    sync::{
        mpsc::{self},
        oneshot,
    },
    task::{JoinHandle, JoinSet},
};
use tokio_util::sync::CancellationToken;

#[derive(Debug)]
enum Command<T: ActivityId> {
    Register {
        spec: ActivitySpec<T>,
        reply: oneshot::Sender<Result<(), SchedulerError>>,
    },
    RegisterMany {
        specs: Vec<ActivitySpec<T>>,
        reply: oneshot::Sender<Result<(), SchedulerError>>,
    },
    Reset {
        spec: ActivitySpec<T>,
    },
    ResetMany {
        specs: Vec<ActivitySpec<T>>,
    },
    Tick,
    SetEnabled {
        id: T,
        enabled: bool,
    },
    ClaimDue {
        reply: oneshot::Sender<Vec<T>>,
    },
    Finish {
        id: T,
        ack: AckMessage,
    },
    Snapshot {
        reply: oneshot::Sender<Vec<Activity<T>>>,
    },
}

#[derive(Debug, Clone)]
pub(crate) struct Scheduler<T>
where
    T: ActivityId,
{
    actor_tx: mpsc::Sender<Command<T>>,
}

impl<T> Scheduler<T>
where
    T: ActivityId,
{
    pub async fn register(&self, spec: ActivitySpec<T>) -> Result<(), SchedulerError> {
        let (reply, rx) = oneshot::channel();
        let _ = self.actor_tx.send(Command::Register { spec, reply }).await;
        rx.await.unwrap()
    }

    pub async fn register_many(&self, specs: Vec<ActivitySpec<T>>) -> Result<(), SchedulerError> {
        let (reply, rx) = oneshot::channel();
        let _ = self.actor_tx.send(Command::RegisterMany { specs, reply }).await;
        rx.await.unwrap()
    }

    pub async fn reset(&self, spec: ActivitySpec<T>) {
        let _ = self.actor_tx.send(Command::Reset { spec }).await;
    }

    pub async fn reset_many(&self, specs: Vec<ActivitySpec<T>>) {
        let _ = self.actor_tx.send(Command::ResetMany { specs }).await;
    }

    pub async fn tick(&self) {
        let _ = self.actor_tx.send(Command::Tick).await;
    }

    pub async fn set_enabled(&self, id: T, enabled: bool) {
        let _ = self.actor_tx.send(Command::SetEnabled { id, enabled }).await;
    }

    pub async fn claim_due(&self) -> Vec<T> {
        let (reply, rx) = oneshot::channel();
        if self.actor_tx.send(Command::ClaimDue { reply }).await.is_err() {
            return Vec::new();
        }
        rx.await.unwrap_or_default()
    }

    pub async fn finish(&self, id: T, ack: AckMessage) {
        let _ = self.actor_tx.send(Command::Finish { id, ack }).await;
    }

    pub async fn snapshot(&self) -> Vec<Activity<T>> {
        let (reply, rx) = oneshot::channel();
        if self.actor_tx.send(Command::Snapshot { reply }).await.is_err() {
            return Vec::new();
        }
        rx.await.unwrap_or_default()
    }

    pub fn run(&self, poll_interval: Duration, cancel_token: CancellationToken) {
        let mut ticker = tokio::time::interval(poll_interval);

        let handle = tokio::spawn({
            let cancel_token = cancel_token.clone();
            async move {
                let mut dispatch = JoinSet::<()>::new();

                loop {
                    select! {
                        _ = cancel_token.cancelled() => break,
                        _ = ticker.tick() => {
                            self.tick().await;

                            let due = self.claim_due().await;

                            for id in due {
                                let sender = sender.clone();
                                let schedule = self.clone();

                                dispatch.spawn(async move {
                                    let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();

                                    let _ = sender.send((id, ack_tx)).await;
                                    let outcome = ack_rx.await.unwrap_or(AckMessage::Done);

                                    schedule.finish(id, outcome).await;
                                });
                            }
                        },
                        Some(_) = dispatch.join_next() => {}
                    }
                }

                let _ = tokio::time::timeout(Duration::from_secs(5), async {
                    while dispatch.join_next().await.is_some() {}
                })
                .await;

                dispatch.abort_all();

                while dispatch.join_next().await.is_some() {}
            }
        });
    }
}

pub struct ScheduleDriver<T: ActivityId> {
    sender: tokio::sync::mpsc::Sender<Ack<T>>,
    receiver: Option<tokio::sync::mpsc::Receiver<Ack<T>>>,
}

struct SchedulerActor<T>
where
    T: ActivityId,
{
    activities: HashMap<T, Activity<T>>,
    clock: Arc<dyn Clock>,
}

impl<T> SchedulerActor<T>
where
    T: ActivityId,
{
    fn send(&mut self, cmd: Command<T>) {
        match cmd {
            Command::Register { spec, reply } => {
                if self.activities.contains_key(&spec.id) {
                    let _ = reply.send(Err(SchedulerError::ActivityAlreadyRegistered));
                } else {
                    self.activities
                        .insert(spec.id, Activity::new(spec, self.clock.clone()));

                    let _ = reply.send(Ok(()));
                }
            }

            Command::RegisterMany { specs, reply } => {
                for spec in specs {
                    if self.activities.contains_key(&spec.id) {
                        let _ = reply.send(Err(SchedulerError::ActivityAlreadyRegistered));
                        return;
                    }

                    self.activities
                        .insert(spec.id, Activity::new(spec, self.clock.clone()));
                }

                let _ = reply.send(Ok(()));
            }

            Command::Reset { spec } => {
                self.activities
                    .insert(spec.id, Activity::new(spec, self.clock.clone()));
            }

            Command::ResetMany { specs } => {
                for spec in specs {
                    self.activities
                        .insert(spec.id, Activity::new(spec, self.clock.clone()));
                }
            }

            Command::Tick => {
                self.activities.retain(|_id, activity| {
                    activity.tick();
                    activity.state != ActivityState::Gc
                });
            }

            Command::SetEnabled { id, enabled } => {
                if let Some(activity) = self.activities.get_mut(&id) {
                    activity.set_enabled(enabled);
                }
            }

            Command::ClaimDue { reply } => {
                let mut ids = Vec::new();
                for activity in self.activities.values_mut() {
                    if let Some(claimed) = activity.claim_if_due() {
                        ids.push(claimed);
                    }
                }
                let _ = reply.send(ids);
            }

            Command::Finish { id, ack } => {
                if let Some(activity) = self.activities.get_mut(&id) {
                    activity.finish(ack);
                    if activity.state == ActivityState::Completed {
                        self.activities.remove(&id);
                    }
                }
            }

            Command::Snapshot { reply } => {
                let _ = reply.send(self.activities.values().cloned().collect());
            }
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum SchedulerError {
    #[error("this activity has been registered, maybe you want reset()")]
    ActivityAlreadyRegistered,
}

pub(crate) fn spawn_scheduler<T>(
    buffer: usize,
    cancellation_token: tokio_util::sync::CancellationToken,
    clock: Arc<dyn Clock>,
) -> (Scheduler<T>, JoinHandle<()>)
where
    T: ActivityId,
{
    let (tx, mut rx) = mpsc::channel::<Command<T>>(buffer);

    let handle = tokio::spawn(async move {
        let mut actor = SchedulerActor {
            activities: HashMap::new(),
            clock: clock,
        };

        loop {
            select! {
                _ = cancellation_token.cancelled() => break,
                message = rx.recv() => match message {
                    Some(cmd) => actor.send(cmd),
                    None => break
                }
            }
        }
    });

    (Scheduler { actor_tx: tx }, handle)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        ack::AckMessage,
        activity::ActivityState,
        activity_spec::{ActivitySchedule, ActivitySpec},
        clock::TestClock,
    };
    use chrono::Utc;
    use std::time::Duration;
    use tokio_util::sync::CancellationToken;

    fn fixed(secs: u64) -> ActivitySchedule {
        ActivitySchedule::FixedInterval {
            duration: Duration::from_secs(secs),
        }
    }

    fn spec(id: u32, schedule: ActivitySchedule) -> ActivitySpec<u32> {
        ActivitySpec { id, schedule }
    }

    /// A test harness owning the spawned scheduler actor, its handle to send
    /// commands, and the [`TestClock`] driving it. Time only advances when the
    /// test body calls [`TestClock::advance`], so every assertion is
    /// deterministic and free of sleeps.
    struct Harness {
        schedule: Scheduler<u32>,
        clock: TestClock,
        token: CancellationToken,
        handle: JoinHandle<()>,
    }

    impl Harness {
        fn new() -> Self {
            let clock = TestClock::new(Utc::now());
            let token = CancellationToken::new();
            let (schedule, handle) =
                spawn_scheduler::<u32>(16, token.clone(), Arc::new(clock.clone()));

            Self {
                schedule,
                clock,
                token,
                handle,
            }
        }

        /// Advance the clock, tick every activity, then return the ids the
        /// scheduler considers due. The intermediate `snapshot` is a sync
        /// barrier: because the actor is a single FIFO task, awaiting any
        /// command guarantees the fire-and-forget `tick` was applied first.
        async fn advance_and_claim(&self, by: Duration) -> Vec<u32> {
            self.clock.advance(by);
            self.schedule.tick().await;
            let mut ids = self.schedule.claim_due().await;
            ids.sort();
            ids
        }

        async fn state_of(&self, id: u32) -> Option<ActivityState> {
            self.schedule
                .snapshot()
                .await
                .into_iter()
                .find(|a| a.spec.id == id)
                .map(|a| a.state)
        }

        async fn ids(&self) -> Vec<u32> {
            let mut ids: Vec<u32> = self
                .schedule
                .snapshot()
                .await
                .into_iter()
                .map(|a| a.spec.id)
                .collect();
            ids.sort();
            ids
        }

        async fn shutdown(self) {
            self.token.cancel();
            let _ = self.handle.await;
        }
    }

    // ---- register ---------------------------------------------------------

    #[tokio::test]
    async fn register_succeeds_and_appears_in_snapshot() {
        let h = Harness::new();

        let result = h.schedule.register(spec(1, fixed(60))).await;

        assert!(result.is_ok());
        assert_eq!(h.ids().await, vec![1]);
        assert_eq!(h.state_of(1).await, Some(ActivityState::Idle));

        h.shutdown().await;
    }

    #[tokio::test]
    async fn register_duplicate_id_returns_error() {
        let h = Harness::new();
        h.schedule.register(spec(1, fixed(60))).await.unwrap();

        let result = h.schedule.register(spec(1, fixed(999))).await;

        assert!(matches!(
            result,
            Err(SchedulerError::ActivityAlreadyRegistered)
        ));
        // The original registration is left untouched.
        assert_eq!(h.ids().await, vec![1]);

        h.shutdown().await;
    }

    #[tokio::test]
    async fn register_many_registers_every_spec() {
        let h = Harness::new();

        let result = h
            .schedule
            .register_many(vec![
                spec(1, fixed(60)),
                spec(2, fixed(120)),
                spec(3, fixed(180)),
            ])
            .await;

        assert!(result.is_ok());
        assert_eq!(h.ids().await, vec![1, 2, 3]);

        h.shutdown().await;
    }

    #[tokio::test]
    async fn register_many_with_duplicate_returns_error() {
        let h = Harness::new();
        h.schedule.register(spec(1, fixed(60))).await.unwrap();

        let result = h
            .schedule
            .register_many(vec![spec(2, fixed(60)), spec(1, fixed(60))])
            .await;

        assert!(matches!(
            result,
            Err(SchedulerError::ActivityAlreadyRegistered)
        ));

        h.shutdown().await;
    }

    // ---- claim_due / tick -------------------------------------------------

    #[tokio::test]
    async fn claim_due_is_empty_with_no_activities() {
        let h = Harness::new();

        assert!(h.schedule.claim_due().await.is_empty());

        h.shutdown().await;
    }

    #[tokio::test]
    async fn activity_not_claimed_before_interval_elapses() {
        let h = Harness::new();
        h.schedule.register(spec(1, fixed(100))).await.unwrap();

        let due = h.advance_and_claim(Duration::from_secs(99)).await;

        assert!(due.is_empty());
        assert_eq!(h.state_of(1).await, Some(ActivityState::Idle));

        h.shutdown().await;
    }

    #[tokio::test]
    async fn activity_claimed_once_interval_elapses() {
        let h = Harness::new();
        h.schedule.register(spec(1, fixed(100))).await.unwrap();

        let due = h.advance_and_claim(Duration::from_secs(100)).await;

        assert_eq!(due, vec![1]);
        assert_eq!(h.state_of(1).await, Some(ActivityState::Running));

        h.shutdown().await;
    }

    #[tokio::test]
    async fn claimed_activity_is_not_reclaimed_until_finished() {
        let h = Harness::new();
        h.schedule.register(spec(1, fixed(100))).await.unwrap();

        let first = h.advance_and_claim(Duration::from_secs(100)).await;
        assert_eq!(first, vec![1]);

        // Still Running, so a further tick + claim yields nothing.
        let second = h.advance_and_claim(Duration::from_secs(100)).await;
        assert!(second.is_empty());

        h.shutdown().await;
    }

    #[tokio::test]
    async fn claim_due_returns_only_the_activities_that_are_due() {
        let h = Harness::new();
        h.schedule
            .register_many(vec![spec(1, fixed(50)), spec(2, fixed(200))])
            .await
            .unwrap();

        let due = h.advance_and_claim(Duration::from_secs(100)).await;

        assert_eq!(
            due,
            vec![1],
            "only the 50s activity should be due at t=100s"
        );
        assert_eq!(h.state_of(1).await, Some(ActivityState::Running));
        assert_eq!(h.state_of(2).await, Some(ActivityState::Idle));

        h.shutdown().await;
    }

    #[tokio::test]
    async fn random_interval_is_not_due_below_min_and_due_at_max() {
        let h = Harness::new();
        h.schedule
            .register(spec(
                1,
                ActivitySchedule::RandomInterval {
                    min: Duration::from_secs(10),
                    max: Duration::from_secs(20),
                },
            ))
            .await
            .unwrap();

        // Below the minimum the target can never have been reached.
        assert!(h.advance_and_claim(Duration::from_secs(9)).await.is_empty());
        // At/above the maximum the target is guaranteed to have been reached.
        assert_eq!(h.advance_and_claim(Duration::from_secs(11)).await, vec![1]);

        h.shutdown().await;
    }

    // ---- set_enabled ------------------------------------------------------

    #[tokio::test]
    async fn disabled_activity_does_not_accumulate_and_is_not_due() {
        let h = Harness::new();
        h.schedule.register(spec(1, fixed(100))).await.unwrap();
        h.schedule.set_enabled(1, false).await;

        let due = h.advance_and_claim(Duration::from_secs(100)).await;

        assert!(due.is_empty());
        assert_eq!(
            h.state_of(1).await,
            Some(ActivityState::Disabled(Box::new(ActivityState::Idle)))
        );

        h.shutdown().await;
    }

    #[tokio::test]
    async fn re_enabled_activity_resumes_scheduling() {
        let h = Harness::new();
        h.schedule.register(spec(1, fixed(100))).await.unwrap();

        h.schedule.set_enabled(1, false).await;
        assert!(
            h.advance_and_claim(Duration::from_secs(100))
                .await
                .is_empty()
        );

        h.schedule.set_enabled(1, true).await;
        // The disabled window did not accumulate, so it must elapse afresh.
        let due = h.advance_and_claim(Duration::from_secs(100)).await;

        assert_eq!(due, vec![1]);

        h.shutdown().await;
    }

    #[tokio::test]
    async fn set_enabled_on_unknown_id_is_a_noop() {
        let h = Harness::new();

        h.schedule.set_enabled(42, false).await;

        // Reaching here (snapshot replies) proves the actor did not panic.
        assert!(h.ids().await.is_empty());

        h.shutdown().await;
    }

    // ---- finish -----------------------------------------------------------

    #[tokio::test]
    async fn finish_done_reschedules_a_fixed_interval_activity() {
        let h = Harness::new();
        h.schedule.register(spec(1, fixed(100))).await.unwrap();

        assert_eq!(h.advance_and_claim(Duration::from_secs(100)).await, vec![1]);
        h.schedule.finish(1, AckMessage::Done).await;

        assert_eq!(h.state_of(1).await, Some(ActivityState::Idle));
        // After re-elapsing the interval it becomes due again.
        assert_eq!(h.advance_and_claim(Duration::from_secs(100)).await, vec![1]);

        h.shutdown().await;
    }

    #[tokio::test]
    async fn finish_snooze_reschedules_for_the_snooze_duration() {
        let h = Harness::new();
        h.schedule.register(spec(1, fixed(100))).await.unwrap();
        assert_eq!(h.advance_and_claim(Duration::from_secs(100)).await, vec![1]);

        h.schedule
            .finish(1, AckMessage::Snooze(Duration::from_secs(30)))
            .await;
        assert_eq!(
            h.state_of(1).await,
            Some(ActivityState::Snoozed(Duration::from_secs(30)))
        );

        // Not due before the snooze elapses, due once it does.
        assert!(
            h.advance_and_claim(Duration::from_secs(29))
                .await
                .is_empty()
        );
        assert_eq!(h.advance_and_claim(Duration::from_secs(1)).await, vec![1]);

        h.shutdown().await;
    }

    #[tokio::test]
    async fn finish_done_on_scheduled_activity_removes_it() {
        let start = Utc::now();
        let clock = TestClock::new(start);
        let token = CancellationToken::new();
        let (schedule, handle) = spawn_scheduler::<u32>(16, token.clone(), Arc::new(clock.clone()));

        let target = start + chrono::Duration::seconds(100);
        schedule
            .register(spec(1, ActivitySchedule::Scheduled { date: target }))
            .await
            .unwrap();

        clock.advance(Duration::from_secs(100));
        schedule.tick().await;
        assert_eq!(schedule.claim_due().await, vec![1]);

        schedule.finish(1, AckMessage::Done).await;

        // A completed one-shot activity is dropped from the scheduler.
        assert!(schedule.snapshot().await.is_empty());

        token.cancel();
        let _ = handle.await;
    }

    #[tokio::test]
    async fn ticking_disabled_scheduled_task_past_target_garbage_collects() {
        let start = Utc::now();
        let clock = TestClock::new(start);
        let token = CancellationToken::new();
        let (schedule, handle) = spawn_scheduler::<u32>(16, token.clone(), Arc::new(clock.clone()));

        let target = start + chrono::Duration::seconds(100);
        schedule
            .register(spec(1, ActivitySchedule::Scheduled { date: target }))
            .await
            .unwrap();

        let _ = schedule.set_enabled(1, false).await;

        clock.advance(Duration::from_secs(50));
        schedule.tick().await;

        assert_eq!(schedule.snapshot().await.len(), 1);
        assert_eq!(schedule.claim_due().await.len(), 0);

        clock.advance(Duration::from_secs(51));
        schedule.tick().await;

        assert_eq!(schedule.snapshot().await.len(), 0);
        assert_eq!(schedule.claim_due().await.len(), 0);

        token.cancel();
        let _ = handle.await;
    }

    #[tokio::test]
    async fn finish_snooze_on_scheduled_activity_keeps_it() {
        let start = Utc::now();
        let clock = TestClock::new(start);
        let token = CancellationToken::new();
        let (schedule, handle) = spawn_scheduler::<u32>(16, token.clone(), Arc::new(clock.clone()));

        let target = start + chrono::Duration::seconds(100);
        schedule
            .register(spec(1, ActivitySchedule::Scheduled { date: target }))
            .await
            .unwrap();

        clock.advance(Duration::from_secs(100));
        schedule.tick().await;
        assert_eq!(schedule.claim_due().await, vec![1]);

        schedule
            .finish(1, AckMessage::Snooze(Duration::from_secs(30)))
            .await;

        let state = schedule
            .snapshot()
            .await
            .into_iter()
            .find(|a| a.spec.id == 1)
            .map(|a| a.state);
        assert_eq!(state, Some(ActivityState::Snoozed(Duration::from_secs(30))));

        token.cancel();
        let _ = handle.await;
    }

    #[tokio::test]
    async fn finish_on_unknown_id_is_a_noop() {
        let h = Harness::new();
        h.schedule.register(spec(1, fixed(100))).await.unwrap();

        h.schedule.finish(99, AckMessage::Done).await;

        assert_eq!(h.ids().await, vec![1]);
        assert_eq!(h.state_of(1).await, Some(ActivityState::Idle));

        h.shutdown().await;
    }

    // ---- snapshot & lifecycle --------------------------------------------

    #[tokio::test]
    async fn snapshot_returns_all_registered_activities() {
        let h = Harness::new();
        h.schedule
            .register_many(vec![spec(1, fixed(60)), spec(2, fixed(120))])
            .await
            .unwrap();

        let snapshot = h.schedule.snapshot().await;

        assert_eq!(snapshot.len(), 2);

        h.shutdown().await;
    }

    #[tokio::test]
    async fn commands_after_cancellation_degrade_gracefully() {
        let h = Harness::new();
        h.schedule.register(spec(1, fixed(60))).await.unwrap();

        h.token.cancel();
        let _ = h.handle.await;

        // The actor is gone; reply-bearing commands fall back to defaults
        // instead of panicking.
        assert!(h.schedule.claim_due().await.is_empty());
        assert!(h.schedule.snapshot().await.is_empty());
    }
}
