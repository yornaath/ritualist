use std::time::Duration;

use chrono::{DateTime, Utc};


#[derive(Debug, Clone, PartialEq, Eq, Copy)]
pub enum ActivationTarget {
  Duration(Duration),
  Date(DateTime<Utc>)
}