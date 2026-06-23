use crate::{
    ack::AckMessage,
    activation_target::ActivationTarget,
    activity_spec::{ActivitySchedule, ActivitySpec},
    clock::{Clock, Instant},
};
use chrono::{DateTime, Utc};
use std::{fmt::Debug, hash::Hash, sync::Arc, time::Duration};

pub trait ActivityId: Debug + Eq + Hash + Copy + Send + 'static {}
impl<T: Debug + Eq + Hash + Copy + Send + 'static> ActivityId for T {}

#[derive(Debug, Clone)]
pub struct Activity<T>
where
    T: ActivityId,
{
    /// The spec the activity was created from
    pub spec: ActivitySpec<T>,
    /// The current state of the activity
    pub state: ActivityState,
    /// For interval tasks this represents the elapsed clock time, used to check against `activation_target`
    /// to see if the activity should run or not.
    pub duration_delta: Duration,
    /// The target for the activation of the activity. Either an elapsed duration for interval tasks or
    /// dates for scheduled tasks.
    pub activation_target: ActivationTarget,
    /// The last monotonic tick time
    pub last_tick: Instant,
    /// When the task last run, not used. But can be handy for consumers.
    pub last_run_at: Option<DateTime<Utc>>,
    /// When the task was created, not used. But can be handy for consumers.
    pub created_at: DateTime<Utc>,
    /// Internal clock used to schedule.
    clock: Arc<dyn Clock>,
}

#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone, PartialEq)]
pub enum ActivityState {
    Idle,
    Running,
    Snoozed(Duration),
    Disabled(Box<ActivityState>),
    Completed,
    Gc,
}

impl ActivityState {
    pub(crate) fn is_scheduled(&self) -> bool {
        match self {
            ActivityState::Idle | ActivityState::Snoozed(_) => true,
            _ => false,
        }
    }
}

impl<T> Activity<T>
where
    T: ActivityId,
{
    /// Create a new [`Activity`]
    pub(crate) fn new(spec: ActivitySpec<T>, clock: Arc<dyn Clock>) -> Self {
        let activation_target: ActivationTarget = spec.clone().schedule.into();
        Self {
            spec,
            state: ActivityState::Idle,
            duration_delta: Duration::ZERO,
            activation_target,
            last_tick: clock.now(),
            last_run_at: None,
            created_at: clock.now_utc(),
            clock,
        }
    }

    /// Tick the activity
    ///
    /// This is used to advance the time of the activity.
    ///
    /// This will update the last_tick and set the current duration delta to the duration
    /// between now and the last tick.
    pub(crate) fn tick(&mut self) {
        let now = self.clock.now();
        let tick_delta = now.saturating_duration_since(self.last_tick);

        self.last_tick = now;

        if self.state.is_scheduled() {
            self.duration_delta += tick_delta;
        }

        self.try_gc_mark()
    }

    /// Mark the activity as garbage collectable if the task meets the conditions:
    /// - It is a schedule Date activity
    /// - And the activity is disabled
    /// - And the activity has reached its target date.
    pub(crate) fn try_gc_mark(&mut self) {
        if matches!(self.spec.schedule, ActivitySchedule::Scheduled { date: _ })
            && matches!(self.state, ActivityState::Disabled(_))
            && self.is_at_target()
        {
            self.state = ActivityState::Gc;
        }
    }

    /// Tries to claim the [`Activity`] if its due to run.
    ///
    /// If its due to run the [`Activity`] will be transitioned into its [`ActivityState::Running`] state,
    /// and the id of the activity will be returned as a `Some(T)`
    ///
    /// If its not due `None`` will be returned
    pub(crate) fn claim_if_due(&mut self) -> Option<T> {
        if self.should_run() {
            self.state = ActivityState::Running;
            self.duration_delta = Duration::ZERO;
            Some(self.spec.id)
        } else {
            None
        }
    }

    /// Set the enabled state of the activity: _**indempotent safe**_
    ///
    /// - When **enabling** will unwrap the previous state from the disabled state(if any) and
    ///   set the state back to what it was before it was disabled.
    /// - When **disabling** will set the current state to disabled wrapping the current state so it
    ///   can be unwrapped and reverted when re-enabling.
    pub(crate) fn set_enabled(&mut self, enabled: bool) {
        if enabled {
            if let ActivityState::Disabled(prev_state) = &self.state {
                self.state = *prev_state.clone();
            }
        } else if !matches!(self.state, ActivityState::Disabled(_)) {
            let prev_state = Box::new(self.state.clone());
            self.state = ActivityState::Disabled(prev_state);
        }
    }

    /// Determine if the activity should run at this given `tick`
    ///
    /// Only `Idle` and `Snoozed` activities are eligible to run.
    /// If eligible it will check either
    /// - the internal `duration_delta` against the `duration_target` for [`ActivationTarget::Duration`]
    /// - that the `target_date` is greater than now for [`ActivationTarget::Date`]
    pub(crate) fn should_run(&self) -> bool {
        if self.state != ActivityState::Idle && !matches!(self.state, ActivityState::Snoozed(_)) {
            return false;
        }

        self.is_at_target()
    }

    /// Check if the activity has reached its target duration(interval) or scheduled Date.
    pub(crate) fn is_at_target(&self) -> bool {
        match self.activation_target {
            ActivationTarget::Duration(duration_target) => self.duration_delta >= duration_target,
            ActivationTarget::Date(target_date) => {
                let now = self.clock.now_utc();
                now >= target_date
            }
        }
    }

    /// Sets the activation target to be immediate.
    /// For durations sets it to ZERO so it fires immediatly.
    /// And for dates set its to NOW for same behavior
    pub(crate) fn set_immediate(&mut self) {
        match self.activation_target {
            ActivationTarget::Duration(_) => {
                self.activation_target = ActivationTarget::Duration(Duration::ZERO)
            }
            ActivationTarget::Date(_) => {
                self.activation_target = ActivationTarget::Date(self.clock.now_utc())
            }
        };
    }

    /// Apply the finish state transition to the activity
    ///
    /// Takes a [`AckMessage`] and transitions the activity into its correct state.
    /// - When [`AckMessage:Done`] put the activity into [`ActivityState::Idle`]
    ///   and calculate next activation target duration based on the activity spec.
    /// - When [`AckMessage:Snooze(duration)`] put the  activity into [`ActivityState::Snoozed(duration)`]
    ///   and calculate next activation target duration based on the snooze duration
    /// - When the schedule is a date based scheduled task and the activity is not snoozed
    ///   then set the activity into its [`ActivityState::Completed`] state
    pub(crate) fn finish(&mut self, ack: AckMessage) {
        let (next_state, duration_target) = match ack {
            AckMessage::Snooze(duration) => (
                ActivityState::Snoozed(duration),
                ActivationTarget::Duration(duration),
            ),
            AckMessage::Done => (ActivityState::Idle, self.spec.schedule.into()),
        };

        if matches!(self.spec.schedule, ActivitySchedule::Scheduled { date: _ })
            && !matches!(next_state, ActivityState::Snoozed(_))
        {
            self.state = ActivityState::Completed;
        } else {
            self.last_run_at = Some(self.clock.now_utc());
            self.activation_target = duration_target;
            self.duration_delta = Duration::ZERO;
            self.state = next_state
        }
    }

    /// Returns how close this activity is to firing, as a percentage in the
    /// range `0.0..=100.0`.
    ///
    /// The meaning of "progress" depends on how the activity is scheduled:
    ///
    /// - For interval schedules ([`ActivitySchedule::FixedInterval`] and
    ///   [`ActivitySchedule::RandomInterval`]), progress is the elapsed time
    ///   accumulated while the activity ticks, relative to the target interval.
    ///   It resets to `0.0` each time the activity runs.
    /// - For one-shot schedules ([`ActivitySchedule::Scheduled`]), progress is
    ///   the fraction of wall-clock time elapsed between when the activity was
    ///   created ([`created_at`](Self::created_at)) and its target date.
    /// ```
    pub fn calculate_progress_pct(&self) -> f64 {
        match self.activation_target {
            ActivationTarget::Duration(duration_target) => {
                (100.0 / duration_target.as_secs_f64()) * self.duration_delta.as_secs_f64()
            }
            ActivationTarget::Date(target_date) => {
                let now = self.clock.now_utc();
                let total = target_date - self.created_at;
                let elapsed = now - self.created_at;

                elapsed.num_milliseconds() as f64 / total.num_milliseconds() as f64 * 100.0
            }
        }
        .clamp(0.0, 100.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::TestClock;

    const ID: u32 = 1;

    fn fixed(secs: u64) -> ActivitySchedule {
        ActivitySchedule::FixedInterval {
            duration: Duration::from_secs(secs),
        }
    }

    fn scheduled(date: DateTime<Utc>) -> ActivitySchedule {
        ActivitySchedule::Scheduled { date }
    }

    /// Build an activity wired to `clock`. The activity's `created_at` and
    /// `last_tick` are sampled from the clock at construction time.
    fn build(clock: &TestClock, schedule: ActivitySchedule) -> Activity<u32> {
        Activity::new(ActivitySpec { id: ID, schedule }, Arc::new(clock.clone()))
    }

    fn duration_target(activity: &Activity<u32>) -> Duration {
        match activity.activation_target {
            ActivationTarget::Duration(d) => d,
            ActivationTarget::Date(_) => panic!("expected a duration target"),
        }
    }

    // ---- construction -----------------------------------------------------

    #[test]
    fn new_fixed_interval_starts_idle_with_duration_target() {
        let clock = TestClock::new(Utc::now());

        let activity = build(&clock, fixed(60));

        assert_eq!(activity.state, ActivityState::Idle);
        assert_eq!(activity.duration_delta, Duration::ZERO);
        assert_eq!(activity.last_run_at, None);
        assert_eq!(
            activity.activation_target,
            ActivationTarget::Duration(Duration::from_secs(60))
        );
    }

    #[test]
    fn new_scheduled_starts_idle_with_date_target() {
        let start = Utc::now();
        let target = start + chrono::Duration::seconds(60);
        let clock = TestClock::new(start);

        let activity = build(&clock, scheduled(target));

        assert_eq!(activity.state, ActivityState::Idle);
        assert_eq!(activity.activation_target, ActivationTarget::Date(target));
        assert_eq!(activity.created_at, start);
    }

    #[test]
    fn new_random_interval_picks_target_within_range() {
        let clock = TestClock::new(Utc::now());
        let schedule = ActivitySchedule::RandomInterval {
            min: Duration::from_secs(10),
            max: Duration::from_secs(20),
        };

        let target = duration_target(&build(&clock, schedule));

        assert!(
            (Duration::from_secs(10)..Duration::from_secs(20)).contains(&target),
            "random target {target:?} should fall within [10s, 20s)"
        );
    }

    // ---- tick / progress accumulation ------------------------------------

    #[test]
    fn tick_accumulates_elapsed_time_while_idle() {
        let clock = TestClock::new(Utc::now());
        let mut activity = build(&clock, fixed(100));

        clock.advance(Duration::from_secs(30));
        activity.tick();

        assert_eq!(activity.duration_delta, Duration::from_secs(30));
    }

    #[test]
    fn tick_accumulates_across_multiple_advances() {
        let clock = TestClock::new(Utc::now());
        let mut activity = build(&clock, fixed(100));

        clock.advance(Duration::from_secs(10));
        activity.tick();
        clock.advance(Duration::from_secs(15));
        activity.tick();

        assert_eq!(activity.duration_delta, Duration::from_secs(25));
    }

    #[test]
    fn tick_progresses_while_snoozed() {
        let clock = TestClock::new(Utc::now());
        let mut activity = build(&clock, fixed(100));
        activity.state = ActivityState::Snoozed(Duration::from_secs(100));

        clock.advance(Duration::from_secs(40));
        activity.tick();

        assert_eq!(activity.duration_delta, Duration::from_secs(40));
    }

    #[test]
    fn tick_does_not_progress_while_running() {
        let clock = TestClock::new(Utc::now());
        let mut activity = build(&clock, fixed(100));
        activity.state = ActivityState::Running;

        clock.advance(Duration::from_secs(40));
        activity.tick();

        assert_eq!(activity.duration_delta, Duration::ZERO);
    }

    #[test]
    fn tick_does_not_progress_while_disabled() {
        let clock = TestClock::new(Utc::now());
        let mut activity = build(&clock, fixed(100));
        activity.set_enabled(false);

        clock.advance(Duration::from_secs(40));
        activity.tick();

        assert_eq!(activity.duration_delta, Duration::ZERO);
    }

    // ---- should_run -------------------------------------------------------

    #[test]
    fn duration_activity_not_due_before_target() {
        let clock = TestClock::new(Utc::now());
        let mut activity = build(&clock, fixed(100));

        clock.advance(Duration::from_secs(99));
        activity.tick();

        assert!(!activity.should_run());
    }

    #[test]
    fn duration_activity_due_when_delta_meets_target() {
        let clock = TestClock::new(Utc::now());
        let mut activity = build(&clock, fixed(100));

        clock.advance(Duration::from_secs(100));
        activity.tick();

        assert!(activity.should_run());
    }

    #[test]
    fn snoozed_activity_becomes_due_after_snooze_elapses() {
        let clock = TestClock::new(Utc::now());
        let mut activity = build(&clock, fixed(100));
        activity.state = ActivityState::Snoozed(Duration::from_secs(30));
        activity.activation_target = ActivationTarget::Duration(Duration::from_secs(30));

        clock.advance(Duration::from_secs(30));
        activity.tick();

        assert!(activity.should_run());
    }

    #[test]
    fn date_activity_not_due_before_target_date() {
        let start = Utc::now();
        let target = start + chrono::Duration::seconds(100);
        let clock = TestClock::new(start);
        let activity = build(&clock, scheduled(target));

        clock.advance(Duration::from_secs(99));

        assert!(!activity.should_run());
    }

    #[test]
    fn date_activity_due_at_target_date() {
        let start = Utc::now();
        let target = start + chrono::Duration::seconds(100);
        let clock = TestClock::new(start);
        let activity = build(&clock, scheduled(target));

        clock.advance(Duration::from_secs(100));

        assert!(activity.should_run());
    }

    #[test]
    fn not_due_when_running_even_if_target_reached() {
        let clock = TestClock::new(Utc::now());
        let mut activity = build(&clock, fixed(100));
        clock.advance(Duration::from_secs(100));
        activity.tick();
        activity.state = ActivityState::Running;

        assert!(!activity.should_run());
    }

    #[test]
    fn not_due_when_completed() {
        let clock = TestClock::new(Utc::now());
        let mut activity = build(&clock, fixed(100));
        clock.advance(Duration::from_secs(100));
        activity.tick();
        activity.state = ActivityState::Completed;

        assert!(!activity.should_run());
    }

    // ---- claim_if_due --------------------------------------------------------

    #[test]
    fn claim_if_due_marks_running_and_returns_id_when_due() {
        let clock = TestClock::new(Utc::now());
        let mut activity = build(&clock, fixed(100));
        clock.advance(Duration::from_secs(100));
        activity.tick();

        let claimed = activity.claim_if_due();

        assert_eq!(claimed, Some(ID));
        assert_eq!(activity.state, ActivityState::Running);
        assert_eq!(activity.duration_delta, Duration::ZERO);
    }

    #[test]
    fn claim_if_due_returns_none_and_leaves_state_when_not_due() {
        let clock = TestClock::new(Utc::now());
        let mut activity = build(&clock, fixed(100));
        clock.advance(Duration::from_secs(50));
        activity.tick();

        let claimed = activity.claim_if_due();

        assert_eq!(claimed, None);
        assert_eq!(activity.state, ActivityState::Idle);
        assert_eq!(activity.duration_delta, Duration::from_secs(50));
    }

    // ---- set_enabled ------------------------------------------------------

    #[test]
    fn disable_wraps_current_state() {
        let clock = TestClock::new(Utc::now());
        let mut activity = build(&clock, fixed(100));

        activity.set_enabled(false);

        assert_eq!(
            activity.state,
            ActivityState::Disabled(Box::new(ActivityState::Idle))
        );
    }

    #[test]
    fn enable_restores_previous_state() {
        let clock = TestClock::new(Utc::now());
        let mut activity = build(&clock, fixed(100));
        let snoozed = ActivityState::Snoozed(Duration::from_secs(5));
        activity.state = snoozed.clone();

        activity.set_enabled(false);
        activity.set_enabled(true);

        assert_eq!(activity.state, snoozed);
    }

    #[test]
    fn disable_is_idempotent() {
        let clock = TestClock::new(Utc::now());
        let mut activity = build(&clock, fixed(100));

        activity.set_enabled(false);
        activity.set_enabled(false);

        assert_eq!(
            activity.state,
            ActivityState::Disabled(Box::new(ActivityState::Idle)),
            "disabling twice must not nest Disabled states"
        );
    }

    #[test]
    fn enable_when_not_disabled_is_noop() {
        let clock = TestClock::new(Utc::now());
        let mut activity = build(&clock, fixed(100));

        activity.set_enabled(true);

        assert_eq!(activity.state, ActivityState::Idle);
    }

    // ---- finish -----------------------------------------------------------

    #[test]
    fn finish_done_on_fixed_interval_resets_to_idle_and_records_run() {
        let clock = TestClock::new(Utc::now());
        let mut activity = build(&clock, fixed(100));
        clock.advance(Duration::from_secs(100));
        activity.tick();
        activity.claim_if_due();

        activity.finish(AckMessage::Done);

        assert_eq!(activity.state, ActivityState::Idle);
        assert_eq!(activity.duration_delta, Duration::ZERO);
        assert_eq!(
            activity.activation_target,
            ActivationTarget::Duration(Duration::from_secs(100))
        );
        assert!(activity.last_run_at.is_some());
    }

    #[test]
    fn finish_snooze_sets_snoozed_state_and_target() {
        let clock = TestClock::new(Utc::now());
        let mut activity = build(&clock, fixed(100));
        let snooze = Duration::from_secs(30);

        activity.finish(AckMessage::Snooze(snooze));

        assert_eq!(activity.state, ActivityState::Snoozed(snooze));
        assert_eq!(
            activity.activation_target,
            ActivationTarget::Duration(snooze)
        );
        assert_eq!(activity.duration_delta, Duration::ZERO);
        assert!(activity.last_run_at.is_some());
    }

    #[test]
    fn finish_done_on_scheduled_marks_completed() {
        let start = Utc::now();
        let target = start + chrono::Duration::seconds(100);
        let clock = TestClock::new(start);
        let mut activity = build(&clock, scheduled(target));

        activity.finish(AckMessage::Done);

        assert_eq!(activity.state, ActivityState::Completed);
        assert_eq!(
            activity.last_run_at, None,
            "a completed one-shot activity records no further run"
        );
    }

    #[test]
    fn finish_snooze_on_scheduled_does_not_complete() {
        let start = Utc::now();
        let target = start + chrono::Duration::seconds(100);
        let clock = TestClock::new(start);
        let mut activity = build(&clock, scheduled(target));
        let snooze = Duration::from_secs(30);

        activity.finish(AckMessage::Snooze(snooze));

        assert_eq!(activity.state, ActivityState::Snoozed(snooze));
        assert_eq!(
            activity.activation_target,
            ActivationTarget::Duration(snooze)
        );
    }

    // ---- is_at_target -----------------------------------------------------

    #[test]
    fn is_at_target_false_for_duration_below_target() {
        let clock = TestClock::new(Utc::now());
        let mut activity = build(&clock, fixed(100));

        clock.advance(Duration::from_secs(99));
        activity.tick();

        assert!(!activity.is_at_target());
    }

    #[test]
    fn is_at_target_true_for_duration_at_target() {
        let clock = TestClock::new(Utc::now());
        let mut activity = build(&clock, fixed(100));

        clock.advance(Duration::from_secs(100));
        activity.tick();

        assert!(activity.is_at_target());
    }

    #[test]
    fn is_at_target_true_for_duration_over_target() {
        let clock = TestClock::new(Utc::now());
        let mut activity = build(&clock, fixed(100));

        clock.advance(Duration::from_secs(150));
        activity.tick();

        assert!(activity.is_at_target());
    }

    #[test]
    fn is_at_target_false_for_date_before_target() {
        let start = Utc::now();
        let target = start + chrono::Duration::seconds(100);
        let clock = TestClock::new(start);
        let activity = build(&clock, scheduled(target));

        clock.advance(Duration::from_secs(99));

        assert!(!activity.is_at_target());
    }

    #[test]
    fn is_at_target_true_for_date_at_target() {
        let start = Utc::now();
        let target = start + chrono::Duration::seconds(100);
        let clock = TestClock::new(start);
        let activity = build(&clock, scheduled(target));

        clock.advance(Duration::from_secs(100));

        assert!(activity.is_at_target());
    }

    #[test]
    fn is_at_target_true_for_date_after_target() {
        let start = Utc::now();
        let target = start + chrono::Duration::seconds(100);
        let clock = TestClock::new(start);
        let activity = build(&clock, scheduled(target));

        clock.advance(Duration::from_secs(150));

        assert!(activity.is_at_target());
    }

    // ---- try_gc_mark ------------------------------------------------------

    #[test]
    fn try_gc_mark_marks_disabled_scheduled_at_target_as_gc() {
        let start = Utc::now();
        let target = start + chrono::Duration::seconds(100);
        let clock = TestClock::new(start);
        let mut activity = build(&clock, scheduled(target));
        activity.set_enabled(false);

        clock.advance(Duration::from_secs(100));
        activity.try_gc_mark();

        assert_eq!(activity.state, ActivityState::Gc);
    }

    #[test]
    fn try_gc_mark_leaves_disabled_scheduled_before_target() {
        let start = Utc::now();
        let target = start + chrono::Duration::seconds(100);
        let clock = TestClock::new(start);
        let mut activity = build(&clock, scheduled(target));
        activity.set_enabled(false);

        clock.advance(Duration::from_secs(50));
        activity.try_gc_mark();

        assert_eq!(
            activity.state,
            ActivityState::Disabled(Box::new(ActivityState::Idle))
        );
    }

    #[test]
    fn try_gc_mark_leaves_enabled_scheduled_at_target() {
        let start = Utc::now();
        let target = start + chrono::Duration::seconds(100);
        let clock = TestClock::new(start);
        let mut activity = build(&clock, scheduled(target));

        clock.advance(Duration::from_secs(100));
        activity.try_gc_mark();

        assert_eq!(
            activity.state,
            ActivityState::Idle,
            "an enabled scheduled activity must not be garbage collected"
        );
    }

    #[test]
    fn try_gc_mark_leaves_disabled_interval_at_target() {
        let clock = TestClock::new(Utc::now());
        let mut activity = build(&clock, fixed(100));
        activity.duration_delta = Duration::from_secs(100);
        activity.set_enabled(false);

        activity.try_gc_mark();

        assert_eq!(
            activity.state,
            ActivityState::Disabled(Box::new(ActivityState::Idle)),
            "interval activities are never garbage collected"
        );
    }

    #[test]
    fn tick_marks_disabled_scheduled_at_target_as_gc() {
        let start = Utc::now();
        let target = start + chrono::Duration::seconds(100);
        let clock = TestClock::new(start);
        let mut activity = build(&clock, scheduled(target));
        activity.set_enabled(false);

        clock.advance(Duration::from_secs(100));
        activity.tick();

        assert_eq!(activity.state, ActivityState::Gc);
    }

    // ---- calculate_progress_pct ------------------------------------------

    #[test]
    fn progress_is_zero_at_start() {
        let clock = TestClock::new(Utc::now());
        let activity = build(&clock, fixed(100));

        assert_eq!(activity.calculate_progress_pct(), 0.0);
    }

    #[test]
    fn progress_is_half_at_midpoint_of_duration() {
        let clock = TestClock::new(Utc::now());
        let mut activity = build(&clock, fixed(100));

        clock.advance(Duration::from_secs(50));
        activity.tick();

        assert!((activity.calculate_progress_pct() - 50.0).abs() < 1e-9);
    }

    #[test]
    fn progress_is_clamped_to_100_when_over_target() {
        let clock = TestClock::new(Utc::now());
        let mut activity = build(&clock, fixed(100));

        clock.advance(Duration::from_secs(250));
        activity.tick();

        assert_eq!(activity.calculate_progress_pct(), 100.0);
    }

    #[test]
    fn progress_tracks_elapsed_fraction_for_date_schedule() {
        let start = Utc::now();
        let target = start + chrono::Duration::seconds(100);
        let clock = TestClock::new(start);
        let activity = build(&clock, scheduled(target));

        clock.advance(Duration::from_secs(50));

        assert!((activity.calculate_progress_pct() - 50.0).abs() < 1e-6);
    }
}
