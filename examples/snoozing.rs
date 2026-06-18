use ritualist::{
    ack::AckMessage,
    activity_spec::{ActivitySpec, ActivitySchedule},
    Ritualist,
};
use std::{
    thread::Result,
    time::Duration,
};
use tokio::{
    sync::{self},
};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Copy)]
pub enum Activity {
    Ping
}

impl Activity {
    pub fn run(&self, ack: sync::oneshot::Sender<AckMessage>) {
        tokio::spawn({
            let activity = self.clone();
            async move {
                match activity {
                    Activity::Ping => {
                        if rand::random_bool(1.0 / 3.0) {
                            println!("snoozing for 5 sec");
                            let _ = ack.send(AckMessage::Snooze(Duration::from_secs(5)));
                        } else {
                            println!("pinged");
                            let _ = ack.send(AckMessage::Done);
                        }
                    }
                }
            }
        });
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let mut ritualist = Ritualist::new(64, Duration::from_millis(100));

    ritualist
        .register_many(vec![
            ActivitySpec {
                id: Activity::Ping,
                schedule: ActivitySchedule::FixedInterval {
                    duration: Duration::from_secs(1),
                },
            }
        ])
        .await
        .expect("Could not put activities onto ritualist.");

    let mut channel = ritualist.take_channel();
    
    ritualist.run();
    
    let listener = tokio::spawn({
        async move {
            while let Some((activity, ack)) = channel.recv().await {
                activity.run(ack);
            }
        }
    });

    let _ = listener.await;

    Ok(())
}
