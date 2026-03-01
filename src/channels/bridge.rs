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
use crate::security::pairing::constant_time_eq;
use anyhow::{Context, Result};
use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::RwLock;
use tokio_tungstenite::tungstenite::Message as WsMessage;

// ── AllowList ────────────────────────────────────────────────────

/// Sender-ID allow list, mirroring the Nostr channel pattern.
#[derive(Debug, Clone)]
enum AllowList {
    /// `"*"` — accept messages from any sender.
    Any,
    /// Accept only these specific sender IDs. Empty vec = deny-all.
    Set(Vec<String>),
}

impl AllowList {
    fn parse(raw: &[String]) -> Self {
        if raw.iter().any(|s| s == "*") {
            Self::Any
        } else {
            Self::Set(raw.to_vec())
        }
    }

    fn is_allowed(&self, sender_id: &str) -> bool {
        match self {
            Self::Any => true,
            Self::Set(ids) => ids.iter().any(|id| id == sender_id),
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
    tx: tokio::sync::mpsc::UnboundedSender<String>,
}

// ── BridgeChannel ────────────────────────────────────────────────

/// WebSocket server channel that bridges third-party systems into ZeroClaw.
pub struct BridgeChannel {
    host: String,
    port: u16,
    token: String,
    allowed: AllowList,
    stream_mode: StreamMode,
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
    ) -> Self {
        Self {
            host,
            port,
            token,
            allowed: AllowList::parse(allowed_senders),
            stream_mode,
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
            if handle.tx.send(json.to_owned()).is_err() {
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
        let addr = format!("{}:{}", self.host, self.port);
        let listener = TcpListener::bind(&addr)
            .await
            .with_context(|| format!("Bridge: failed to bind {addr}"))?;

        tracing::info!("Bridge channel listening on {addr}");

        loop {
            let (stream, peer) = match listener.accept().await {
                Ok(s) => s,
                Err(e) => {
                    tracing::warn!("Bridge: accept error: {e}");
                    continue;
                }
            };

            tracing::debug!("Bridge: new TCP connection from {peer}");

            let ws_stream = match tokio_tungstenite::accept_async(stream).await {
                Ok(ws) => ws,
                Err(e) => {
                    tracing::warn!("Bridge: WS upgrade failed from {peer}: {e}");
                    continue;
                }
            };

            let conn_id = uuid::Uuid::new_v4().to_string();
            let token = self.token.clone();
            let allowed = self.allowed.clone();
            let connections = Arc::clone(&self.connections);
            let sender_routing = Arc::clone(&self.sender_routing);
            let tx = tx.clone();

            tokio::spawn(async move {
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
                // Cleanup on disconnect.
                connections.write().await.remove(&conn_id);
                sender_routing
                    .write()
                    .await
                    .retain(|_, cid| cid.as_str() != conn_id);
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

    async fn finalize_draft(
        &self,
        recipient: &str,
        message_id: &str,
        text: &str,
    ) -> Result<()> {
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

    async fn add_reaction(
        &self,
        channel_id: &str,
        message_id: &str,
        emoji: &str,
    ) -> Result<()> {
        let frame = OutboundFrame::ReactionAdd {
            channel_id,
            message_id,
            emoji,
        };
        // Reactions are broadcast to all connections since they aren't sender-specific.
        let json = serde_json::to_string(&frame)?;
        let conns = self.connections.read().await;
        for handle in conns.values() {
            let _ = handle.tx.send(json.clone());
        }
        Ok(())
    }

    async fn remove_reaction(
        &self,
        channel_id: &str,
        message_id: &str,
        emoji: &str,
    ) -> Result<()> {
        let frame = OutboundFrame::ReactionRemove {
            channel_id,
            message_id,
            emoji,
        };
        let json = serde_json::to_string(&frame)?;
        let conns = self.connections.read().await;
        for handle in conns.values() {
            let _ = handle.tx.send(json.clone());
        }
        Ok(())
    }
}

// ── Per-connection task ──────────────────────────────────────────

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

    // ── Authentication handshake ─────────────────────────────────
    let auth_ok = loop {
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
                break constant_time_eq(&token, &expected_token);
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
    let (out_tx, mut out_rx) = tokio::sync::mpsc::unbounded_channel::<String>();
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
                    let _ = handle.tx.send(err_frame);
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
                        let _ = handle.tx.send(err_frame);
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
                    let _ = handle.tx.send(pong);
                }
            }
        }
    }

    writer.abort();
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
        let json =
            r#"{"type":"message","content":"hi","sender_id":"user_a","thread_id":"t-1"}"#;
        let frame: InboundFrame = serde_json::from_str(json).unwrap();
        assert!(
            matches!(frame, InboundFrame::Message { thread_id, .. } if thread_id.as_deref() == Some("t-1"))
        );
    }

    #[test]
    fn inbound_approval_response_deserializes() {
        let json =
            r#"{"type":"approval_response","request_id":"r1","approved":true,"sender_id":"user_a"}"#;
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

    #[test]
    fn bridge_channel_name_is_bridge() {
        let ch = BridgeChannel::new(
            "127.0.0.1".to_string(),
            9090,
            "tok".to_string(),
            &[],
            StreamMode::Off,
        );
        assert_eq!(ch.name(), "bridge");
    }

    #[test]
    fn bridge_channel_draft_support_follows_stream_mode() {
        let off = BridgeChannel::new(
            "127.0.0.1".to_string(),
            9090,
            "tok".to_string(),
            &[],
            StreamMode::Off,
        );
        assert!(!off.supports_draft_updates());

        let partial = BridgeChannel::new(
            "127.0.0.1".to_string(),
            9090,
            "tok".to_string(),
            &[],
            StreamMode::Partial,
        );
        assert!(partial.supports_draft_updates());
    }

    #[tokio::test]
    async fn health_check_false_with_no_connections() {
        let ch = BridgeChannel::new(
            "127.0.0.1".to_string(),
            9090,
            "tok".to_string(),
            &[],
            StreamMode::Off,
        );
        assert!(!ch.health_check().await);
    }

    #[tokio::test]
    async fn send_to_sender_fails_without_connection() {
        let ch = BridgeChannel::new(
            "127.0.0.1".to_string(),
            9090,
            "tok".to_string(),
            &[],
            StreamMode::Off,
        );
        let result = ch
            .send(&SendMessage::new("hello", "user_a"))
            .await;
        assert!(result.is_err());
    }
}
