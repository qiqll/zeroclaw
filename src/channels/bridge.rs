//! Bridge channel — WebSocket server for third-party system integration.
//!
//! A single WS connection represents one external system. Each inbound message
//! carries a `sender_id` field so the connection can multiplex traffic for many
//! end-users. ZeroClaw maintains per-sender conversation history keyed by
//! `bridge_{sender_id}`, identical to other channel behaviours.
//!
//! ## Protocol
//!
//! All frames are JSON text. The `"type"` field discriminates message kinds.
//!
//! **Connection lifecycle:**
//! 1. Client connects to `ws://{host}:{port}`.
//! 2. Client sends `{"type":"auth","token":"<token>"}` as its first message.
//! 3. Server replies `{"type":"auth_result","success":true}` or closes.
//! 4. Bidirectional communication begins.

use crate::channels::traits::{Channel, ChannelMessage, SendMessage};
use crate::config::schema::StreamMode;
use crate::security::pairing::{constant_time_eq, is_public_bind};
use anyhow::{Context, Result};
use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::RwLock;
use tokio_tungstenite::tungstenite::Message as WsMessage;

// ── AllowList ────────────────────────────────────────────────────

/// Sender-ID allow list, mirroring the Nostr channel pattern.
#[derive(Debug, Clone)]
enum AllowList {
    /// `"*"` — accept messages from any sender.
    Any,
    /// Accept only these specific sender IDs. Empty set = deny-all.
    Set(HashSet<String>),
}

impl AllowList {
    fn parse(raw: &[String]) -> Self {
        if raw.iter().any(|s| s == "*") {
            Self::Any
        } else {
            Self::Set(raw.iter().cloned().collect())
        }
    }

    fn is_allowed(&self, sender_id: &str) -> bool {
        match self {
            Self::Any => true,
            Self::Set(ids) => ids.contains(sender_id),
        }
    }
}

// ── Protocol types ───────────────────────────────────────────────

/// Inbound (client → ZeroClaw) message envelope.
#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum InboundFrame {
    Auth {
        token: String,
    },
    Message {
        content: String,
        sender_id: String,
        #[serde(default)]
        thread_id: Option<String>,
    },
    ApprovalResponse {
        request_id: String,
        approved: bool,
        sender_id: String,
    },
    Ping,
}

/// Outbound (ZeroClaw → client) message envelope.
#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum OutboundFrame<'a> {
    AuthResult {
        success: bool,
        #[serde(skip_serializing_if = "Option::is_none")]
        error: Option<&'a str>,
    },
    Message {
        content: &'a str,
        sender_id: &'a str,
        #[serde(skip_serializing_if = "Option::is_none")]
        thread_ts: Option<&'a str>,
    },
    TypingStart {
        sender_id: &'a str,
    },
    TypingStop {
        sender_id: &'a str,
    },
    DraftStart {
        draft_id: &'a str,
        content: &'a str,
        sender_id: &'a str,
    },
    DraftUpdate {
        draft_id: &'a str,
        content: &'a str,
        sender_id: &'a str,
    },
    DraftFinalize {
        draft_id: &'a str,
        content: &'a str,
        sender_id: &'a str,
    },
    DraftCancel {
        draft_id: &'a str,
        sender_id: &'a str,
    },
    ApprovalPrompt {
        request_id: &'a str,
        tool_name: &'a str,
        arguments: &'a serde_json::Value,
        sender_id: &'a str,
    },
    ReactionAdd {
        channel_id: &'a str,
        message_id: &'a str,
        emoji: &'a str,
    },
    ReactionRemove {
        channel_id: &'a str,
        message_id: &'a str,
        emoji: &'a str,
    },
    Error {
        message: &'a str,
    },
    Pong,
}

// ── Connection handle ────────────────────────────────────────────

/// Write-half handle for one authenticated WS connection.
struct ConnectionHandle {
    tx: tokio::sync::mpsc::Sender<String>,
}

// ── BridgeChannel ────────────────────────────────────────────────

/// WebSocket server channel that bridges third-party systems into ZeroClaw.
pub struct BridgeChannel {
    host: String,
    port: u16,
    token: String,
    allowed: AllowList,
    stream_mode: StreamMode,
    max_connections: u16,
    allow_public_bind: bool,
    /// connection_id (UUID) → write handle.
    connections: Arc<RwLock<HashMap<String, ConnectionHandle>>>,
    /// sender_id → connection_id routing table.
    sender_routing: Arc<RwLock<HashMap<String, String>>>,
}

impl BridgeChannel {
    pub fn new(
        host: String,
        port: u16,
        token: String,
        allowed_senders: &[String],
        stream_mode: StreamMode,
        max_connections: u16,
        allow_public_bind: bool,
    ) -> Self {
        Self {
            host,
            port,
            token,
            allowed: AllowList::parse(allowed_senders),
            stream_mode,
            max_connections,
            allow_public_bind,
            connections: Arc::new(RwLock::new(HashMap::new())),
            sender_routing: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Send a serialized JSON frame to the connection that last routed
    /// `sender_id`. Removes stale connections on send failure.
    async fn send_to_sender(&self, sender_id: &str, json: &str) -> Result<()> {
        let routing = self.sender_routing.read().await;
        let conn_id = routing
            .get(sender_id)
            .cloned()
            .context("No connection for sender")?;
        drop(routing);

        let conns = self.connections.read().await;
        if let Some(handle) = conns.get(&conn_id) {
            if handle.tx.try_send(json.to_owned()).is_err() {
                drop(conns);
                self.remove_connection(&conn_id).await;
                anyhow::bail!("Bridge connection closed for sender {sender_id}");
            }
        } else {
            anyhow::bail!("Bridge connection {conn_id} not found");
        }
        Ok(())
    }

    /// Serialize an outbound frame and send it to a sender.
    async fn send_frame(&self, sender_id: &str, frame: &OutboundFrame<'_>) -> Result<()> {
        let json = serde_json::to_string(frame)?;
        self.send_to_sender(sender_id, &json).await
    }

    /// Remove a connection and all its sender routing entries.
    async fn remove_connection(&self, conn_id: &str) {
        self.connections.write().await.remove(conn_id);
        self.sender_routing
            .write()
            .await
            .retain(|_, cid| cid != conn_id);
    }
}

#[async_trait]
impl Channel for BridgeChannel {
    fn name(&self) -> &str {
        "bridge"
    }

    async fn send(&self, message: &SendMessage) -> Result<()> {
        let frame = OutboundFrame::Message {
            content: &message.content,
            sender_id: &message.recipient,
            thread_ts: message.thread_ts.as_deref(),
        };
        self.send_frame(&message.recipient, &frame).await
    }

    async fn listen(&self, tx: tokio::sync::mpsc::Sender<ChannelMessage>) -> Result<()> {
        // ── H3: Refuse public bind without explicit opt-in ──
        if is_public_bind(&self.host) && !self.allow_public_bind {
            anyhow::bail!(
                "Bridge: refusing to bind to {} — would be exposed to the network.\n\
                 Fix: use host = \"127.0.0.1\" (default) or set allow_public_bind = true \
                 in [channels.bridge] (NOT recommended).",
                self.host
            );
        }

        let addr = format!("{}:{}", self.host, self.port);
        let listener = TcpListener::bind(&addr)
            .await
            .with_context(|| format!("Bridge: failed to bind {addr}"))?;

        tracing::info!("Bridge channel listening on {addr}");

        // ── H2: Limit concurrent connections via semaphore ──
        let semaphore = Arc::new(tokio::sync::Semaphore::new(self.max_connections.into()));

        loop {
            let (stream, peer) = match listener.accept().await {
                Ok(s) => s,
                Err(e) => {
                    tracing::warn!("Bridge: accept error: {e}");
                    continue;
                }
            };

            let permit = match semaphore.clone().try_acquire_owned() {
                Ok(p) => p,
                Err(_) => {
                    tracing::warn!(
                        "Bridge: max connections ({}) reached, rejecting {peer}",
                        self.max_connections
                    );
                    drop(stream);
                    continue;
                }
            };

            tracing::debug!("Bridge: new TCP connection from {peer}");

            let conn_id = uuid::Uuid::new_v4().to_string();
            let token = self.token.clone();
            let allowed = self.allowed.clone();
            let connections = Arc::clone(&self.connections);
            let sender_routing = Arc::clone(&self.sender_routing);
            let tx = tx.clone();

            tokio::spawn(async move {
                // Perform WS upgrade inside spawn with timeout so a slow
                // handshake cannot block the accept loop.
                let ws_stream = match tokio::time::timeout(
                    WS_UPGRADE_TIMEOUT,
                    tokio_tungstenite::accept_async(stream),
                )
                .await
                {
                    Ok(Ok(ws)) => ws,
                    Ok(Err(e)) => {
                        tracing::warn!("Bridge: WS upgrade failed from {peer}: {e}");
                        return;
                    }
                    Err(_) => {
                        tracing::warn!("Bridge: WS upgrade timed out from {peer}");
                        return;
                    }
                };

                if let Err(e) = handle_connection(
                    ws_stream,
                    conn_id.clone(),
                    token,
                    allowed,
                    connections.clone(),
                    sender_routing.clone(),
                    tx,
                )
                .await
                {
                    tracing::debug!("Bridge: connection {conn_id} ended: {e}");
                }
                // Cleanup: handle_connection already removes the connection
                // handle to trigger graceful writer shutdown. The remove
                // here is defensive (covers early-exit / panic paths) and
                // is idempotent (HashMap::remove on a missing key is a no-op).
                connections.write().await.remove(&conn_id);
                sender_routing
                    .write()
                    .await
                    .retain(|_, cid| cid.as_str() != conn_id);
                // Drop the semaphore permit to allow new connections.
                drop(permit);
            });
        }
    }

    async fn health_check(&self) -> bool {
        // Healthy if at least one authenticated connection exists.
        !self.connections.read().await.is_empty()
    }

    async fn start_typing(&self, recipient: &str) -> Result<()> {
        self.send_frame(
            recipient,
            &OutboundFrame::TypingStart {
                sender_id: recipient,
            },
        )
        .await
    }

    async fn stop_typing(&self, recipient: &str) -> Result<()> {
        self.send_frame(
            recipient,
            &OutboundFrame::TypingStop {
                sender_id: recipient,
            },
        )
        .await
    }

    fn supports_draft_updates(&self) -> bool {
        self.stream_mode == StreamMode::Partial
    }

    async fn send_draft(&self, message: &SendMessage) -> Result<Option<String>> {
        let draft_id = format!("bridge_draft_{}", uuid::Uuid::new_v4());
        let frame = OutboundFrame::DraftStart {
            draft_id: &draft_id,
            content: &message.content,
            sender_id: &message.recipient,
        };
        self.send_frame(&message.recipient, &frame).await?;
        Ok(Some(draft_id))
    }

    async fn update_draft(
        &self,
        recipient: &str,
        message_id: &str,
        text: &str,
    ) -> Result<Option<String>> {
        let frame = OutboundFrame::DraftUpdate {
            draft_id: message_id,
            content: text,
            sender_id: recipient,
        };
        self.send_frame(recipient, &frame).await?;
        Ok(None)
    }

    async fn finalize_draft(&self, recipient: &str, message_id: &str, text: &str) -> Result<()> {
        let frame = OutboundFrame::DraftFinalize {
            draft_id: message_id,
            content: text,
            sender_id: recipient,
        };
        self.send_frame(recipient, &frame).await
    }

    async fn cancel_draft(&self, recipient: &str, message_id: &str) -> Result<()> {
        let frame = OutboundFrame::DraftCancel {
            draft_id: message_id,
            sender_id: recipient,
        };
        self.send_frame(recipient, &frame).await
    }

    async fn send_approval_prompt(
        &self,
        recipient: &str,
        request_id: &str,
        tool_name: &str,
        arguments: &serde_json::Value,
        _thread_ts: Option<String>,
    ) -> Result<()> {
        let frame = OutboundFrame::ApprovalPrompt {
            request_id,
            tool_name,
            arguments,
            sender_id: recipient,
        };
        self.send_frame(recipient, &frame).await
    }

    async fn add_reaction(&self, channel_id: &str, message_id: &str, emoji: &str) -> Result<()> {
        let frame = OutboundFrame::ReactionAdd {
            channel_id,
            message_id,
            emoji,
        };
        // Reactions are broadcast to all connections since they aren't sender-specific.
        let json = serde_json::to_string(&frame)?;
        let conns = self.connections.read().await;
        let mut stale: Vec<String> = Vec::new();
        for (conn_id, handle) in conns.iter() {
            if handle.tx.try_send(json.clone()).is_err() {
                stale.push(conn_id.clone());
            }
        }
        drop(conns);
        for conn_id in &stale {
            self.remove_connection(conn_id).await;
        }
        Ok(())
    }

    async fn remove_reaction(&self, channel_id: &str, message_id: &str, emoji: &str) -> Result<()> {
        let frame = OutboundFrame::ReactionRemove {
            channel_id,
            message_id,
            emoji,
        };
        let json = serde_json::to_string(&frame)?;
        let conns = self.connections.read().await;
        let mut stale: Vec<String> = Vec::new();
        for (conn_id, handle) in conns.iter() {
            if handle.tx.try_send(json.clone()).is_err() {
                stale.push(conn_id.clone());
            }
        }
        drop(conns);
        for conn_id in &stale {
            self.remove_connection(conn_id).await;
        }
        Ok(())
    }
}

// ── Per-connection task ──────────────────────────────────────────

/// Timeout for the WebSocket upgrade handshake (TCP → WS).
const WS_UPGRADE_TIMEOUT: Duration = Duration::from_secs(10);

/// Auth handshake timeout.
const AUTH_TIMEOUT: Duration = Duration::from_secs(10);

/// Outbound message queue size per connection. If the queue fills up
/// (WS writer cannot keep pace), new messages are dropped.
const OUTBOUND_QUEUE_SIZE: usize = 1024;

/// Drive one WebSocket connection: authenticate, then relay messages.
async fn handle_connection(
    ws_stream: tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>,
    conn_id: String,
    expected_token: String,
    allowed: AllowList,
    connections: Arc<RwLock<HashMap<String, ConnectionHandle>>>,
    sender_routing: Arc<RwLock<HashMap<String, String>>>,
    tx: tokio::sync::mpsc::Sender<ChannelMessage>,
) -> Result<()> {
    let (mut ws_tx, mut ws_rx) = ws_stream.split();

    // ── Authentication handshake (H1: with timeout) ─────────────
    let auth_result = tokio::time::timeout(AUTH_TIMEOUT, async {
        loop {
            let msg = ws_rx
                .next()
                .await
                .context("Bridge: connection closed before auth")?
                .context("Bridge: WS read error during auth")?;

            let text = match msg {
                WsMessage::Text(t) => t.to_string(),
                WsMessage::Ping(_) | WsMessage::Pong(_) => continue,
                WsMessage::Close(_) => anyhow::bail!("Bridge: client closed during auth"),
                _ => continue,
            };

            let frame: InboundFrame =
                serde_json::from_str(&text).context("Bridge: invalid JSON during auth")?;

            match frame {
                InboundFrame::Auth { token } => {
                    break Ok(constant_time_eq(&token, &expected_token));
                }
                InboundFrame::Ping => {
                    let pong = serde_json::to_string(&OutboundFrame::Pong)?;
                    ws_tx.send(WsMessage::Text(pong.into())).await?;
                    continue;
                }
                _ => {
                    // Non-auth message before authentication — reject.
                    let reject = serde_json::to_string(&OutboundFrame::AuthResult {
                        success: false,
                        error: Some("auth required as first message"),
                    })?;
                    ws_tx.send(WsMessage::Text(reject.into())).await?;
                    anyhow::bail!("Bridge: non-auth message received before authentication");
                }
            }
        }
    })
    .await;

    let auth_ok = match auth_result {
        Ok(inner) => inner?,
        Err(_) => {
            tracing::warn!("Bridge: auth timeout for connection {conn_id}");
            anyhow::bail!("Bridge: auth handshake timed out");
        }
    };

    if !auth_ok {
        let reject = serde_json::to_string(&OutboundFrame::AuthResult {
            success: false,
            error: Some("invalid token"),
        })?;
        ws_tx.send(WsMessage::Text(reject.into())).await?;
        anyhow::bail!("Bridge: auth failed for connection {conn_id}");
    }

    let ok = serde_json::to_string(&OutboundFrame::AuthResult {
        success: true,
        error: None,
    })?;
    ws_tx.send(WsMessage::Text(ok.into())).await?;
    tracing::info!("Bridge: connection {conn_id} authenticated");

    // ── Register connection ──────────────────────────────────────
    let (out_tx, mut out_rx) = tokio::sync::mpsc::channel::<String>(OUTBOUND_QUEUE_SIZE);
    connections
        .write()
        .await
        .insert(conn_id.clone(), ConnectionHandle { tx: out_tx });

    // Spawn writer task: drains outbound queue → WS.
    let writer = tokio::spawn(async move {
        while let Some(json) = out_rx.recv().await {
            if ws_tx.send(WsMessage::Text(json.into())).await.is_err() {
                break;
            }
        }
        // Attempt graceful WS close frame when the channel drains.
        let _ = ws_tx.close().await;
    });

    // ── Message loop ─────────────────────────────────────────────
    while let Some(result) = ws_rx.next().await {
        let msg = match result {
            Ok(m) => m,
            Err(e) => {
                tracing::debug!("Bridge: WS read error on {conn_id}: {e}");
                break;
            }
        };

        let text = match msg {
            WsMessage::Text(t) => t.to_string(),
            WsMessage::Ping(_) | WsMessage::Pong(_) => continue,
            WsMessage::Close(_) => break,
            _ => continue,
        };

        let frame: InboundFrame = match serde_json::from_str(&text) {
            Ok(f) => f,
            Err(e) => {
                tracing::warn!("Bridge: invalid JSON on {conn_id}: {e}");
                // Send error frame but keep connection alive.
                let err_frame = serde_json::to_string(&OutboundFrame::Error {
                    message: "invalid JSON frame",
                })?;
                let conns = connections.read().await;
                if let Some(handle) = conns.get(&conn_id) {
                    let _ = handle.tx.try_send(err_frame);
                }
                continue;
            }
        };

        match frame {
            InboundFrame::Auth { .. } => {
                // Already authenticated — ignore duplicate auth.
                tracing::debug!("Bridge: ignoring duplicate auth on {conn_id}");
            }
            InboundFrame::Message {
                content,
                sender_id,
                thread_id,
            } => {
                if !allowed.is_allowed(&sender_id) {
                    tracing::warn!(
                        "Bridge: rejected message from unauthorized sender: {sender_id}"
                    );
                    let err_frame = serde_json::to_string(&OutboundFrame::Error {
                        message: "sender not allowed",
                    })?;
                    let conns = connections.read().await;
                    if let Some(handle) = conns.get(&conn_id) {
                        let _ = handle.tx.try_send(err_frame);
                    }
                    continue;
                }

                // Update sender → connection routing.
                sender_routing
                    .write()
                    .await
                    .insert(sender_id.clone(), conn_id.clone());

                let timestamp = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_secs())
                    .unwrap_or(0);

                let channel_msg = ChannelMessage {
                    id: uuid::Uuid::new_v4().to_string(),
                    sender: format!("bridge_{sender_id}"),
                    reply_target: sender_id,
                    content,
                    channel: "bridge".to_string(),
                    timestamp,
                    thread_ts: thread_id,
                };

                if tx.send(channel_msg).await.is_err() {
                    tracing::info!("Bridge: message bus closed, stopping connection {conn_id}");
                    break;
                }
            }
            InboundFrame::ApprovalResponse {
                request_id,
                approved,
                sender_id,
            } => {
                if !allowed.is_allowed(&sender_id) {
                    continue;
                }
                // Route approval as a synthetic channel message with a command prefix
                // that the approval manager can recognize.
                let approval_content = if approved {
                    format!("/approve-allow {request_id}")
                } else {
                    format!("/approve-deny {request_id}")
                };

                let timestamp = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_secs())
                    .unwrap_or(0);

                let channel_msg = ChannelMessage {
                    id: uuid::Uuid::new_v4().to_string(),
                    sender: format!("bridge_{sender_id}"),
                    reply_target: sender_id,
                    content: approval_content,
                    channel: "bridge".to_string(),
                    timestamp,
                    thread_ts: None,
                };

                if tx.send(channel_msg).await.is_err() {
                    break;
                }
            }
            InboundFrame::Ping => {
                let pong = serde_json::to_string(&OutboundFrame::Pong)?;
                let conns = connections.read().await;
                if let Some(handle) = conns.get(&conn_id) {
                    let _ = handle.tx.try_send(pong);
                }
            }
        }
    }

    // ── M2: Graceful shutdown — remove connection handle (drops sender),
    //    then wait for writer to flush and send WS close frame. ──
    connections.write().await.remove(&conn_id);
    let _ = writer.await;
    Ok(())
}

// ── Tests ────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── AllowList ────────────────────────────────────────────────

    #[test]
    fn allow_list_empty_denies_all() {
        let al = AllowList::parse(&[]);
        assert!(!al.is_allowed("user_a"));
    }

    #[test]
    fn allow_list_wildcard_allows_all() {
        let al = AllowList::parse(&["*".to_string()]);
        assert!(al.is_allowed("user_a"));
        assert!(al.is_allowed("anything"));
    }

    #[test]
    fn allow_list_specific_ids() {
        let al = AllowList::parse(&["user_a".to_string(), "user_b".to_string()]);
        assert!(al.is_allowed("user_a"));
        assert!(al.is_allowed("user_b"));
        assert!(!al.is_allowed("user_c"));
    }

    #[test]
    fn allow_list_wildcard_mixed_allows_all() {
        let al = AllowList::parse(&["user_a".to_string(), "*".to_string()]);
        assert!(al.is_allowed("user_a"));
        assert!(al.is_allowed("unknown"));
    }

    // ── Protocol JSON round-trip ─────────────────────────────────

    #[test]
    fn inbound_auth_deserializes() {
        let json = r#"{"type":"auth","token":"secret123"}"#;
        let frame: InboundFrame = serde_json::from_str(json).unwrap();
        assert!(matches!(frame, InboundFrame::Auth { token } if token == "secret123"));
    }

    #[test]
    fn inbound_message_deserializes() {
        let json = r#"{"type":"message","content":"hello","sender_id":"user_a"}"#;
        let frame: InboundFrame = serde_json::from_str(json).unwrap();
        assert!(
            matches!(frame, InboundFrame::Message { content, sender_id, thread_id } if content == "hello" && sender_id == "user_a" && thread_id.is_none())
        );
    }

    #[test]
    fn inbound_message_with_thread_deserializes() {
        let json = r#"{"type":"message","content":"hi","sender_id":"user_a","thread_id":"t-1"}"#;
        let frame: InboundFrame = serde_json::from_str(json).unwrap();
        assert!(
            matches!(frame, InboundFrame::Message { thread_id, .. } if thread_id.as_deref() == Some("t-1"))
        );
    }

    #[test]
    fn inbound_approval_response_deserializes() {
        let json = r#"{"type":"approval_response","request_id":"r1","approved":true,"sender_id":"user_a"}"#;
        let frame: InboundFrame = serde_json::from_str(json).unwrap();
        assert!(
            matches!(frame, InboundFrame::ApprovalResponse { request_id, approved, sender_id } if request_id == "r1" && approved && sender_id == "user_a")
        );
    }

    #[test]
    fn inbound_ping_deserializes() {
        let json = r#"{"type":"ping"}"#;
        let frame: InboundFrame = serde_json::from_str(json).unwrap();
        assert!(matches!(frame, InboundFrame::Ping));
    }

    #[test]
    fn outbound_auth_result_serializes() {
        let frame = OutboundFrame::AuthResult {
            success: true,
            error: None,
        };
        let json = serde_json::to_string(&frame).unwrap();
        assert!(json.contains(r#""type":"auth_result""#));
        assert!(json.contains(r#""success":true"#));
        assert!(!json.contains("error"));
    }

    #[test]
    fn outbound_auth_result_with_error_serializes() {
        let frame = OutboundFrame::AuthResult {
            success: false,
            error: Some("bad token"),
        };
        let json = serde_json::to_string(&frame).unwrap();
        assert!(json.contains(r#""success":false"#));
        assert!(json.contains(r#""error":"bad token""#));
    }

    #[test]
    fn outbound_message_serializes() {
        let frame = OutboundFrame::Message {
            content: "response",
            sender_id: "user_a",
            thread_ts: None,
        };
        let json = serde_json::to_string(&frame).unwrap();
        assert!(json.contains(r#""type":"message""#));
        assert!(json.contains(r#""content":"response""#));
        assert!(json.contains(r#""sender_id":"user_a""#));
        assert!(!json.contains("thread_ts"));
    }

    #[test]
    fn outbound_typing_start_serializes() {
        let frame = OutboundFrame::TypingStart {
            sender_id: "user_a",
        };
        let json = serde_json::to_string(&frame).unwrap();
        assert!(json.contains(r#""type":"typing_start""#));
    }

    #[test]
    fn outbound_draft_start_serializes() {
        let frame = OutboundFrame::DraftStart {
            draft_id: "bridge_draft_123",
            content: "partial",
            sender_id: "user_a",
        };
        let json = serde_json::to_string(&frame).unwrap();
        assert!(json.contains(r#""type":"draft_start""#));
        assert!(json.contains(r#""draft_id":"bridge_draft_123""#));
    }

    #[test]
    fn outbound_approval_prompt_serializes() {
        let args = serde_json::json!({"cmd": "ls"});
        let frame = OutboundFrame::ApprovalPrompt {
            request_id: "r1",
            tool_name: "shell",
            arguments: &args,
            sender_id: "user_a",
        };
        let json = serde_json::to_string(&frame).unwrap();
        assert!(json.contains(r#""type":"approval_prompt""#));
        assert!(json.contains(r#""tool_name":"shell""#));
    }

    #[test]
    fn outbound_pong_serializes() {
        let frame = OutboundFrame::Pong;
        let json = serde_json::to_string(&frame).unwrap();
        assert!(json.contains(r#""type":"pong""#));
    }

    #[test]
    fn outbound_error_serializes() {
        let frame = OutboundFrame::Error {
            message: "something went wrong",
        };
        let json = serde_json::to_string(&frame).unwrap();
        assert!(json.contains(r#""type":"error""#));
        assert!(json.contains(r#""message":"something went wrong""#));
    }

    #[test]
    fn outbound_reaction_add_serializes() {
        let frame = OutboundFrame::ReactionAdd {
            channel_id: "ch1",
            message_id: "msg1",
            emoji: "\u{2705}",
        };
        let json = serde_json::to_string(&frame).unwrap();
        assert!(json.contains(r#""type":"reaction_add""#));
    }

    // ── BridgeChannel construction ───────────────────────────────

    /// Helper to create a `BridgeChannel` with defaults for test use.
    fn test_channel(token: &str, allowed: &[String]) -> BridgeChannel {
        BridgeChannel::new(
            "127.0.0.1".to_string(),
            0, // port irrelevant for unit tests
            token.to_string(),
            allowed,
            StreamMode::Off,
            64,
            false,
        )
    }

    #[test]
    fn bridge_channel_name_is_bridge() {
        let ch = test_channel("tok", &[]);
        assert_eq!(ch.name(), "bridge");
    }

    #[test]
    fn bridge_channel_draft_support_follows_stream_mode() {
        let off = test_channel("tok", &[]);
        assert!(!off.supports_draft_updates());

        let partial = BridgeChannel::new(
            "127.0.0.1".to_string(),
            0,
            "tok".to_string(),
            &[],
            StreamMode::Partial,
            64,
            false,
        );
        assert!(partial.supports_draft_updates());
    }

    #[tokio::test]
    async fn health_check_false_with_no_connections() {
        let ch = test_channel("tok", &[]);
        assert!(!ch.health_check().await);
    }

    #[tokio::test]
    async fn send_to_sender_fails_without_connection() {
        let ch = test_channel("tok", &[]);
        let result = ch.send(&SendMessage::new("hello", "user_a")).await;
        assert!(result.is_err());
    }

    // ── H3: Public bind safety ──────────────────────────────────

    #[tokio::test]
    async fn listen_rejects_public_bind_without_opt_in() {
        let ch = BridgeChannel::new(
            "0.0.0.0".to_string(),
            0,
            "tok".to_string(),
            &[],
            StreamMode::Off,
            64,
            false, // allow_public_bind = false
        );
        let (tx, _rx) = tokio::sync::mpsc::channel(1);
        let result = ch.listen(tx).await;
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("refusing to bind"),
            "Expected public-bind rejection, got: {err_msg}"
        );
    }

    // ── Integration tests (H4) ──────────────────────────────────

    /// Start a bridge listener on an ephemeral port and return the bound address.
    async fn start_bridge_listener(
        token: &str,
        allowed: &[String],
    ) -> (
        std::net::SocketAddr,
        tokio::sync::mpsc::Receiver<ChannelMessage>,
        tokio::task::JoinHandle<Result<()>>,
    ) {
        // Bind to port 0 to get an ephemeral port.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let token = token.to_string();
        let allowed = AllowList::parse(allowed);
        let connections: Arc<RwLock<HashMap<String, ConnectionHandle>>> =
            Arc::new(RwLock::new(HashMap::new()));
        let sender_routing: Arc<RwLock<HashMap<String, String>>> =
            Arc::new(RwLock::new(HashMap::new()));
        let (tx, rx) = tokio::sync::mpsc::channel(16);

        let handle = tokio::spawn(async move {
            let (stream, _peer) = listener.accept().await?;
            let ws_stream = tokio_tungstenite::accept_async(stream).await?;
            let conn_id = "test-conn".to_string();
            handle_connection(
                ws_stream,
                conn_id,
                token,
                allowed,
                connections,
                sender_routing,
                tx,
            )
            .await
        });

        (addr, rx, handle)
    }

    #[tokio::test]
    async fn auth_success_allows_messages() {
        let allowed = vec!["user_a".to_string()];
        let (addr, mut rx, _handle) = start_bridge_listener("test_token", &allowed).await;

        let url = format!("ws://{addr}");
        let (mut ws, _resp) = tokio_tungstenite::connect_async(&url).await.unwrap();

        // Send auth.
        let auth = r#"{"type":"auth","token":"test_token"}"#;
        ws.send(WsMessage::Text(auth.into())).await.unwrap();

        // Read auth_result.
        let resp = ws.next().await.unwrap().unwrap();
        let text = resp.into_text().unwrap();
        assert!(
            text.contains(r#""success":true"#),
            "Expected auth success, got: {text}"
        );

        // Send a message.
        let msg = r#"{"type":"message","content":"hello","sender_id":"user_a"}"#;
        ws.send(WsMessage::Text(msg.into())).await.unwrap();

        // Verify it arrives on the channel bus.
        let channel_msg = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("timed out waiting for message")
            .expect("channel closed");
        assert_eq!(channel_msg.content, "hello");
        assert_eq!(channel_msg.sender, "bridge_user_a");

        ws.close(None).await.ok();
    }

    #[tokio::test]
    async fn auth_failure_closes_connection() {
        let (addr, _rx, handle) = start_bridge_listener("correct_token", &[]).await;

        let url = format!("ws://{addr}");
        let (mut ws, _resp) = tokio_tungstenite::connect_async(&url).await.unwrap();

        // Send wrong token.
        let auth = r#"{"type":"auth","token":"wrong_token"}"#;
        ws.send(WsMessage::Text(auth.into())).await.unwrap();

        // Read auth_result — should be failure.
        let resp = ws.next().await.unwrap().unwrap();
        let text = resp.into_text().unwrap();
        assert!(
            text.contains(r#""success":false"#),
            "Expected auth failure, got: {text}"
        );

        // The server-side handle_connection should return an error.
        let result = handle.await.unwrap();
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn auth_timeout_closes_connection() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let connections: Arc<RwLock<HashMap<String, ConnectionHandle>>> =
            Arc::new(RwLock::new(HashMap::new()));
        let sender_routing: Arc<RwLock<HashMap<String, String>>> =
            Arc::new(RwLock::new(HashMap::new()));
        let (tx, _rx) = tokio::sync::mpsc::channel(1);

        let handle = tokio::spawn(async move {
            let (stream, _peer) = listener.accept().await.unwrap();
            let ws_stream = tokio_tungstenite::accept_async(stream).await.unwrap();
            handle_connection(
                ws_stream,
                "test-conn".to_string(),
                "token".to_string(),
                AllowList::Any,
                connections,
                sender_routing,
                tx,
            )
            .await
        });

        let url = format!("ws://{addr}");
        let (_ws, _resp) = tokio_tungstenite::connect_async(&url).await.unwrap();

        // Don't send any auth — wait for timeout.
        // AUTH_TIMEOUT is 10s; use a generous deadline.
        let result = tokio::time::timeout(Duration::from_secs(15), handle)
            .await
            .expect("handle_connection did not finish within 15s")
            .unwrap();

        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("timed out") || err_msg.contains("closed before auth"),
            "Expected timeout error, got: {err_msg}"
        );
    }

    #[tokio::test]
    async fn unauthorized_sender_rejected() {
        // Only allow "user_a".
        let allowed = vec!["user_a".to_string()];
        let (addr, mut rx, _handle) = start_bridge_listener("tok", &allowed).await;

        let url = format!("ws://{addr}");
        let (mut ws, _resp) = tokio_tungstenite::connect_async(&url).await.unwrap();

        // Auth.
        ws.send(WsMessage::Text(r#"{"type":"auth","token":"tok"}"#.into()))
            .await
            .unwrap();
        let _ = ws.next().await; // consume auth_result

        // Send message as unauthorized sender.
        let msg = r#"{"type":"message","content":"hi","sender_id":"user_b"}"#;
        ws.send(WsMessage::Text(msg.into())).await.unwrap();

        // Read error frame.
        let resp = ws.next().await.unwrap().unwrap();
        let text = resp.into_text().unwrap();
        assert!(
            text.contains("sender not allowed"),
            "Expected sender-rejected error, got: {text}"
        );

        // Verify nothing arrived on the channel bus.
        let result = tokio::time::timeout(Duration::from_millis(200), rx.recv()).await;
        assert!(
            result.is_err(),
            "No message should reach the bus for unauthorized sender"
        );

        ws.close(None).await.ok();
    }

    #[tokio::test]
    async fn connection_cleanup_on_disconnect() {
        let connections: Arc<RwLock<HashMap<String, ConnectionHandle>>> =
            Arc::new(RwLock::new(HashMap::new()));
        let sender_routing: Arc<RwLock<HashMap<String, String>>> =
            Arc::new(RwLock::new(HashMap::new()));

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let conns = Arc::clone(&connections);
        let routing = Arc::clone(&sender_routing);
        let (tx, _rx) = tokio::sync::mpsc::channel(16);
        let allowed = vec!["user_a".to_string()];

        let handle = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let ws_stream = tokio_tungstenite::accept_async(stream).await.unwrap();
            let conn_id = "cleanup-test".to_string();
            let result = handle_connection(
                ws_stream,
                conn_id.clone(),
                "tok".to_string(),
                AllowList::parse(&allowed),
                conns.clone(),
                routing.clone(),
                tx,
            )
            .await;
            // Simulate the cleanup the listen() spawn block does.
            conns.write().await.remove(&conn_id);
            routing
                .write()
                .await
                .retain(|_, cid| cid.as_str() != conn_id);
            result
        });

        let url = format!("ws://{addr}");
        let (mut ws, _) = tokio_tungstenite::connect_async(&url).await.unwrap();

        // Auth + send a message to register routing.
        ws.send(WsMessage::Text(r#"{"type":"auth","token":"tok"}"#.into()))
            .await
            .unwrap();
        let _ = ws.next().await; // auth_result
        ws.send(WsMessage::Text(
            r#"{"type":"message","content":"hi","sender_id":"user_a"}"#.into(),
        ))
        .await
        .unwrap();
        // Small delay so the server processes the message.
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Disconnect.
        ws.close(None).await.ok();

        // Wait for server handler to complete.
        let _ = tokio::time::timeout(Duration::from_secs(3), handle).await;

        // Verify cleanup.
        assert!(
            connections.read().await.is_empty(),
            "connections should be empty after disconnect"
        );
        assert!(
            sender_routing.read().await.is_empty(),
            "sender_routing should be empty after disconnect"
        );
    }
}
