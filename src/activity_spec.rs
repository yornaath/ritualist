use chrono::{DateTime, Utc};
use rand::Rng;
use std::{hash::Hash, time::Duration};

use crate::{activation_target::ActivationTarget, activity::ActivityId};

#[derive(Debug, Clone)]
pub struct ActivitySpec<T: ActivityId> {
    /// The id of the activity [`ActivityId` = Debug + Eq + Hash + Copy + Send + 'static ] 
    /// 
    /// The `Hash` of this will used to identify unique tasks in memory, so registering multiple activities with the same
    /// id hash will overwrite the old entry and reset its scheduler state.
    pub id: T,
    /// The schedule for when the task should run.
    /// 
    /// - [`ActivitySchedule:Fixed { duration }`] - run at a fixed interval
    /// - [`ActivitySchedule:Randmom { min, max }`] - run at a random interval
    /// - [`ActivitySchedule:Scheduled { date } `] - run at a given date time
    pub schedule: ActivitySchedule,
}

impl<T> ActivitySpec<T>
where
    T: ActivityId,
{
    pub(crate) fn validate(&self) -> Result<(), ActivitySpecError> {
        if let ActivitySchedule::RandomInterval { min, max } = self.schedule
            && min >= max
        {
            return Err(ActivitySpecError::IntervalRangeOverflow);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Copy)]
pub enum ActivitySchedule {
    FixedInterval { duration: Duration },
    RandomInterval { min: Duration, max: Duration },
    Scheduled { date: DateTime<Utc>}
}


impl From<ActivitySchedule> for ActivationTarget {
    fn from(val: ActivitySchedule) -> Self {
        match val {
            ActivitySchedule::FixedInterval { duration } => ActivationTarget::Duration(duration),
            ActivitySchedule::RandomInterval { min, max } => ActivationTarget::Duration(rand::rng().random_range(min..max)),
            ActivitySchedule::Scheduled { date } => ActivationTarget::Date(date)
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ActivitySpecError {
    #[error("random interval is invalid: min must be less than max")]
    IntervalRangeOverflow,
}
