use std::time::Duration;

pub type Ack<T> = (T, tokio::sync::oneshot::Sender<AckMessage>);

#[derive(Debug)]
pub enum AckMessage {
    Done,
    Snooze(Duration),
}
