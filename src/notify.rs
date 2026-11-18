//! Sends alerts to Discord, or only logs them when it isn't configured.

use std::time::Duration;

use reqwest::StatusCode;
use serde_json::{Value, json};
use tracing::{error, info};

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Severity {
    Critical,
    Warning,
    Resolved,
    Info,
}

impl Severity {
    pub fn as_str(self) -> &'static str {
        match self {
            Severity::Critical => "critical",
            Severity::Warning => "warning",
            Severity::Resolved => "resolved",
            Severity::Info => "info",
        }
    }

    fn color(self) -> u32 {
        match self {
            Severity::Critical => 0xE5484D,
            Severity::Warning => 0xF5A524,
            Severity::Resolved => 0x30A46C,
            Severity::Info => 0x3E63DD,
        }
    }
}

pub struct Alert {
    pub title: String,
    pub body: String,
    pub severity: Severity,
}

pub struct Discord {
    http: reqwest::Client,
    url: Option<String>,
}

impl Discord {
    pub fn from_env() -> Self {
        Self {
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(10))
                .build()
                .expect("http client"),
            url: std::env::var("DISCORD_WEBHOOK_URL")
                .ok()
                .filter(|v| !v.is_empty()),
        }
    }

    /// A failed alert is only logged, it isn't sent again.
    pub async fn send(&self, alert: &Alert) {
        let Some(url) = self.url.as_deref() else {
            info!(
                "alert (no destination configured): {} {}\n{}",
                alert.severity.as_str(),
                alert.title,
                alert.body
            );
            return;
        };
        let payload = json!({
            "embeds": [{
                "title": truncate(&alert.title, 256),
                "description": truncate(&alert.body, 4000),
                "color": alert.severity.color(),
            }],
        });
        for _ in 0..3 {
            match self.http.post(url).json(&payload).send().await {
                Ok(resp) if resp.status() == StatusCode::TOO_MANY_REQUESTS => {
                    let info: Value = resp.json().await.unwrap_or_default();
                    let wait = info["retry_after"].as_f64().unwrap_or(2.0);
                    tokio::time::sleep(Duration::from_secs_f64(wait)).await;
                }
                Ok(resp) if !resp.status().is_success() => {
                    let status = resp.status();
                    let text = resp.text().await.unwrap_or_default();
                    error!("fail to send alert: {status} {}", truncate(&text, 200));
                    return;
                }
                Ok(_) => return,
                Err(e) => {
                    error!("fail to send alert: {e}");
                    return;
                }
            }
        }
        error!("fail to send alert: still rate limited after 3 tries");
    }
}

fn truncate(text: &str, max_chars: usize) -> String {
    text.chars().take(max_chars).collect()
}
