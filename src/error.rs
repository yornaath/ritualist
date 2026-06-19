use crate::{activity_spec::ActivitySpecError, schedule::SchedulerError};

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum RitualistError {
    #[error("An error with the validation of the ActivitySpec.")]
    ActivitySpecError(ActivitySpecError),
    #[error("An error happend in the scheulder.")]
    ScheulerError(SchedulerError),
}
