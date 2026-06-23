use std::time::Duration;

use chrono::{DateTime, Utc};

#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone, PartialEq, Eq, Copy)]
pub enum ActivationTarget {
    Duration(Duration),
    Date(DateTime<Utc>),
}
