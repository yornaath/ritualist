use std::{
    io::{self, Write},
    sync::Arc,
    time::Duration,
};

use ritualist::{RunningRitualist, activity::ActivityId, schedule::WithScheduler};
use tokio::{sync::Mutex, time::sleep};

struct Frame {
    prev_lines: usize,
}

impl Frame {
    fn new() -> Self {
        Frame { prev_lines: 0 }
    }

    fn render(&mut self, content: &str) {
        let mut out = io::stdout();

        // Move up to the top of the previous frame, then clear downward.
        if self.prev_lines > 0 {
            // \x1b[{n}A = up n lines, \x1b[J = clear from cursor to end of screen
            write!(out, "\x1b[{}A\x1b[J", self.prev_lines).unwrap();
        }

        write!(out, "{}", content).unwrap();
        out.flush().unwrap();

        // Count the lines we just printed for next time.
        self.prev_lines = content.lines().count();
    }
}

pub fn observer<T: ActivityId>(ritulist: Arc<Mutex<RunningRitualist<T>>>) {
    tokio::spawn({
        let ritulist = ritulist.clone();
        async move {
            let mut frame = Frame::new();

            loop {
                sleep(Duration::from_millis(100)).await;
                let activitities = ritulist.lock().await.snapshot().await;

                let mut content = String::new();

                for act in activitities {
                    let line = format!(
                        "{:?}[{:?}: {:?}]: {:?}% \n",
                        act.spec.id,
                        act.state,
                        act.spec.schedule,
                        act.calculate_progress_pct().round()
                    );
                    content.push_str(&line);
                }

                frame.render(&content);

                io::stdout().flush().unwrap();
            }
        }
    });
}
