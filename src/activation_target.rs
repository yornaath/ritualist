use std::time::Duration;

use chrono::{DateTime, Utc};


#[derive(Debug, Clone, PartialEq, Eq, Copy)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum ActivationTarget {
  Duration(Duration),
  Date(DateTime<Utc>)
}