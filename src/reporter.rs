//! Collects what the watch tasks detect and sends it every 30s as one Discord message.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::time::interval;

use crate::notify::{Alert, Discord, Severity};

/// how often what was detected is sent, all in one message
const GROUP_WINDOW: Duration = Duration::from_secs(30);
const MAX_LINES: usize = 30;
/// the message of an event or a node condition is cut so one line stays readable
const MAX_DETAIL_CHARS: usize = 200;

/// The lines for the next message, shared by every watch task. The lock is only held to push a line or to take
/// them all, never while waiting on anything.
#[derive(Clone, Default)]
pub struct Batch(Arc<Mutex<Vec<(Severity, String)>>>);

impl Batch {
    pub fn push(&self, severity: Severity, line: String) {
        self.0.lock().unwrap().push((severity, line));
    }

    /// An alert as one line: its title, then its detail squashed onto the same line and cut.
    pub fn alert(&self, severity: Severity, title: String, detail: &str) {
        let detail: String = detail
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ")
            .chars()
            .take(MAX_DETAIL_CHARS)
            .collect();
        let line = if detail.is_empty() {
            title
        } else {
            format!("{title} · {detail}")
        };
        self.push(severity, line);
    }

    fn take(&self) -> Vec<(Severity, String)> {
        std::mem::take(&mut *self.0.lock().unwrap())
    }
}

pub async fn run(batch: Batch, discord: Discord) {
    let mut tick = interval(GROUP_WINDOW);
    loop {
        tick.tick().await;
        // the lock is already released here, the watch tasks keep pushing while Discord is called
        if let Some(alert) = digest(&batch.take()) {
            discord.send(&alert).await;
        }
    }
}

fn digest(batch: &[(Severity, String)]) -> Option<Alert> {
    let severity = batch
        .iter()
        .map(|(severity, _)| *severity)
        .max_by_key(|s| rank(*s))?;
    let lines: Vec<String> = batch
        .iter()
        .take(MAX_LINES)
        .map(|(severity, line)| format!("- [{}] {line}", severity.as_str()))
        .collect();
    let more = if batch.len() > MAX_LINES {
        format!("\n… and {} more", batch.len() - MAX_LINES)
    } else {
        String::new()
    };
    Some(Alert {
        title: format!(
            "{} alert{}",
            batch.len(),
            if batch.len() == 1 { "" } else { "s" }
        ),
        body: format!("{}{more}", lines.join("\n")),
        severity,
    })
}

/// Which severity colors a message with several lines, the worst one wins.
fn rank(severity: Severity) -> u8 {
    match severity {
        Severity::Critical => 3,
        Severity::Warning => 2,
        Severity::Info => 1,
        Severity::Resolved => 0,
    }
}
