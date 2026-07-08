// Copyright 2026 EmeraldPay Ltd
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Buffered writers delivering serialized log events to a file or a TCP
//! socket. Ports the legacy `FileLogWriter` / `SocketLogWriter` semantics:
//! a bounded queue that drops the newest events when full, periodic flushes,
//! append-only files, and socket reconnects with exponential backoff.

use crate::config::log::{LogEncoding, LogTargetConfig};
use crate::logs::encoding;
use crate::metrics;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc;

/// Queue capacity and per-flush batch limit of the legacy writers.
const QUEUE_LIMIT: usize = 5000;
/// How often the file writer flushes its queue.
const FLUSH_INTERVAL: Duration = Duration::from_millis(250);
/// How long the socket writer waits when it has nothing to send.
const SOCKET_IDLE_SLEEP: Duration = Duration::from_millis(25);
/// Socket reconnect backoff: initial delay, doubling each failure.
const RECONNECT_INITIAL: Duration = Duration::from_millis(100);
const RECONNECT_MAX: Duration = Duration::from_secs(30);

/// A handle submitting serialized events to a background writer task.
///
/// Submission never blocks: when the queue is full the event is dropped, as
/// the legacy `DataQueue` dropped the newest events.
#[derive(Clone)]
pub struct LogWriter {
    tx: mpsc::Sender<Vec<u8>>,
    /// The log category (`access` / `request`): the display name in messages
    /// and the `type` label of the `monitoringLogs_*` metrics.
    label: &'static str,
    dropped: Arc<AtomicU64>,
    queued: Arc<AtomicU64>,
}

impl LogWriter {
    /// Start a writer for the configured target. Returns `None` for a target
    /// that can't be started (e.g. a socket without a port), which the caller
    /// reports; a `None` matches the legacy fallback to a disabled writer.
    pub fn start(config: &LogTargetConfig, label: &'static str) -> Option<LogWriter> {
        match config.target_type.as_str() {
            "file" => {
                let filename = config.filename.clone()?;
                let (writer, rx) = Self::new(QUEUE_LIMIT, label);
                tokio::spawn(run_file(
                    rx,
                    PathBuf::from(filename),
                    label,
                    Arc::clone(&writer.queued),
                ));
                Some(writer)
            }
            "socket" => {
                let Some(port) = config.port else {
                    tracing::error!("No port set for a {} socket target", label);
                    return None;
                };
                let host = config.host.clone().unwrap_or_else(|| "127.0.0.1".to_string());
                let limit = config.buffer.unwrap_or(QUEUE_LIMIT);
                let (writer, rx) = Self::new(limit, label);
                tokio::spawn(run_socket(
                    rx,
                    format!("{host}:{port}"),
                    config.resolved_encoding(),
                    label,
                    Arc::clone(&writer.queued),
                ));
                Some(writer)
            }
            other => {
                tracing::error!("Unsupported {} log target: {}", label, other);
                None
            }
        }
    }

    fn new(limit: usize, label: &'static str) -> (LogWriter, mpsc::Receiver<Vec<u8>>) {
        metrics::log_writer_created(label);
        let (tx, rx) = mpsc::channel(limit);
        (
            LogWriter {
                tx,
                label,
                dropped: Arc::new(AtomicU64::new(0)),
                queued: Arc::new(AtomicU64::new(0)),
            },
            rx,
        )
    }

    /// Queue one serialized event. Drops it when the queue is full.
    /// Returns whether the event was queued.
    pub fn submit(&self, event: Vec<u8>) -> bool {
        metrics::log_produced(self.label);
        match self.tx.try_send(event) {
            Ok(()) => {
                let size = self.queued.fetch_add(1, Ordering::Relaxed) + 1;
                metrics::log_queue_size(self.label, size);
                true
            }
            Err(_) => {
                metrics::log_dropped(self.label);
                // Warn once in a while, not per event — a full queue drops in bursts.
                let dropped = self.dropped.fetch_add(1, Ordering::Relaxed);
                if dropped % 1000 == 0 {
                    tracing::warn!("Log queue is full, dropping events");
                }
                false
            }
        }
    }

    /// Events currently waiting to be written.
    pub fn queue_size(&self) -> u64 {
        self.queued.load(Ordering::Relaxed)
    }
}

/// Report `count` events written to the storage: the collect counter and the
/// shrunk queue gauge.
fn on_written(queued: &AtomicU64, label: &'static str, count: u64) {
    let size = queued.fetch_sub(count, Ordering::Relaxed).saturating_sub(count);
    metrics::log_collected(label, count);
    metrics::log_queue_size(label, size);
}

/// Append-only file writer: wakes on an interval and writes everything queued
/// since the last flush. The file is created if missing and never truncated.
async fn run_file(
    mut rx: mpsc::Receiver<Vec<u8>>,
    path: PathBuf,
    label: &'static str,
    queued: Arc<AtomicU64>,
) {
    let mut pending: Vec<Vec<u8>> = Vec::new();
    let mut interval = tokio::time::interval(FLUSH_INTERVAL);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        interval.tick().await;
        while pending.len() < QUEUE_LIMIT {
            match rx.try_recv() {
                Ok(event) => pending.push(event),
                Err(_) => break,
            }
        }
        if pending.is_empty() {
            continue;
        }

        let mut batch = Vec::new();
        for event in &pending {
            batch.extend_from_slice(&encoding::frame(&LogEncoding::NewLine, event));
        }
        let written = tokio::fs::OpenOptions::new()
            .append(true)
            .create(true)
            .open(&path)
            .await;
        let result = match written {
            Ok(mut file) => file.write_all(&batch).await,
            Err(e) => Err(e),
        };
        match result {
            Ok(()) => {
                on_written(&queued, label, pending.len() as u64);
                pending.clear();
            }
            // Keep the batch for the next flush, like the legacy writer
            // returning unwritten events back to the queue.
            Err(e) => {
                tracing::warn!("Failed to write {} log to {}: {}", label, path.display(), e);
            }
        }
    }
}

/// TCP socket writer: a persistent connection re-established with exponential
/// backoff, sending each event framed with the configured encoding.
async fn run_socket(
    mut rx: mpsc::Receiver<Vec<u8>>,
    addr: String,
    encoding: LogEncoding,
    label: &'static str,
    queued: Arc<AtomicU64>,
) {
    let mut backoff = RECONNECT_INITIAL;
    loop {
        let mut conn = match tokio::net::TcpStream::connect(&addr).await {
            Ok(conn) => conn,
            Err(e) => {
                tracing::warn!(
                    "Failed to connect {} log to {}, retry in {:?}: {}",
                    label,
                    addr,
                    backoff,
                    e
                );
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(RECONNECT_MAX);
                continue;
            }
        };
        tracing::info!("Sending {} log to {}", label, addr);

        // One unsent event survives a broken connection and is retried first.
        let mut unsent: Option<Vec<u8>> = None;
        'connected: loop {
            let event = match unsent.take() {
                Some(event) => event,
                None => match rx.recv().await {
                    Some(event) => event,
                    // The writer handle is gone — nothing more to send.
                    None => return,
                },
            };
            if let Err(e) = conn.write_all(&encoding::frame(&encoding, &event)).await {
                tracing::warn!("Lost {} log connection to {}: {}", label, addr, e);
                unsent = Some(event);
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(RECONNECT_MAX);
                break 'connected;
            }
            on_written(&queued, label, 1);
            backoff = RECONNECT_INITIAL;
            // Batch what's already queued before the next await, so an idle
            // stream doesn't hold a half-written buffer.
            while let Ok(event) = rx.try_recv() {
                if let Err(e) = conn.write_all(&encoding::frame(&encoding, &event)).await {
                    tracing::warn!("Lost {} log connection to {}: {}", label, addr, e);
                    unsent = Some(event);
                    tokio::time::sleep(SOCKET_IDLE_SLEEP).await;
                    break 'connected;
                }
                on_written(&queued, label, 1);
            }
            let _ = conn.flush().await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn file_target(path: &std::path::Path) -> LogTargetConfig {
        LogTargetConfig::file(path.to_str().unwrap())
    }

    #[tokio::test]
    async fn writes_events_as_jsonl() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("events.jsonl");
        let writer = LogWriter::start(&file_target(&path), "test").unwrap();

        assert!(writer.submit(br#"{"n":1}"#.to_vec()));
        assert!(writer.submit(br#"{"n":2}"#.to_vec()));

        // Wait for a flush cycle.
        tokio::time::sleep(Duration::from_millis(600)).await;
        let content = std::fs::read_to_string(&path).unwrap();
        assert_eq!(content, "{\"n\":1}\n{\"n\":2}\n");
        assert_eq!(writer.queue_size(), 0);
    }

    #[tokio::test]
    async fn appends_to_existing_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("events.jsonl");
        std::fs::write(&path, "{\"old\":true}\n").unwrap();

        let writer = LogWriter::start(&file_target(&path), "test").unwrap();
        writer.submit(br#"{"new":true}"#.to_vec());

        tokio::time::sleep(Duration::from_millis(600)).await;
        let content = std::fs::read_to_string(&path).unwrap();
        assert_eq!(content, "{\"old\":true}\n{\"new\":true}\n");
    }

    #[tokio::test]
    async fn socket_receives_newline_framed_events() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let config = LogTargetConfig {
            target_type: "socket".to_string(),
            host: Some(addr.ip().to_string()),
            port: Some(addr.port()),
            encoding: Some("newline".to_string()),
            ..Default::default()
        };
        let writer = LogWriter::start(&config, "test").unwrap();
        writer.submit(br#"{"n":1}"#.to_vec());

        let (mut conn, _) = listener.accept().await.unwrap();
        let mut buf = vec![0u8; 64];
        let n = tokio::io::AsyncReadExt::read(&mut conn, &mut buf).await.unwrap();
        assert_eq!(&buf[..n], b"{\"n\":1}\n");
    }

    #[tokio::test]
    async fn socket_uses_size_prefix_by_default() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let config = LogTargetConfig {
            target_type: "socket".to_string(),
            host: Some(addr.ip().to_string()),
            port: Some(addr.port()),
            ..Default::default()
        };
        let writer = LogWriter::start(&config, "test").unwrap();
        writer.submit(br#"{"n":1}"#.to_vec());

        let (mut conn, _) = listener.accept().await.unwrap();
        let mut buf = vec![0u8; 64];
        let n = tokio::io::AsyncReadExt::read(&mut conn, &mut buf).await.unwrap();
        assert_eq!(&buf[..n], [&[0u8, 0, 0, 7][..], br#"{"n":1}"#].concat());
    }

    #[test]
    fn socket_without_port_is_rejected() {
        let config = LogTargetConfig {
            target_type: "socket".to_string(),
            ..Default::default()
        };
        // Outside a runtime this would panic on spawn if it tried to start.
        assert!(LogWriter::start(&config, "test").is_none());
    }
}
