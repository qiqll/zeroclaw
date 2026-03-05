# Third-Party Channel Integration: openclaw-weibo vs ZeroClaw Bridge Channel

> Comparative architecture analysis based on source code review.
> Date: 2026-03-04

## 1. Project Overview

### openclaw-weibo

| Dimension | Detail |
|-----------|--------|
| **Language** | TypeScript (ES2022), OpenClaw plugin ecosystem |
| **Core deps** | `ws` (WebSocket), `zod` (schema), openclaw >= 2026.3.1 (host) |
| **Code size** | ~14 source files, ~1000 lines of business logic + tests |
| **Architecture** | OpenClaw `ChannelPlugin` — one plugin per platform |
| **Purpose** | Dedicated Weibo DM channel plugin |

### ZeroClaw Bridge Channel

| Dimension | Detail |
|-----------|--------|
| **Language** | Rust, ZeroClaw native trait system |
| **Core deps** | `tokio-tungstenite` (WebSocket), `serde` (serialization), tokio runtime |
| **Code size** | 1 file, 1227 lines (`bridge.rs`) + 311 lines trait + config structs |
| **Architecture** | Generic WebSocket server with neutral transport protocol |
| **Purpose** | Platform-agnostic bridge — one channel serves all platforms |

---

## 2. Fundamental Architecture Difference

### openclaw-weibo: "Platform-Native Plugin"

```
Weibo IM Server ←——[WS connection]——→ openclaw-weibo plugin ←——[plugin API]——→ OpenClaw Agent
                                           ↑
                                     Embeds all platform logic:
                                     • Weibo OAuth token fetch/refresh
                                     • Weibo WS frame parsing
                                     • Message dedup (messageId Set)
                                     • Multi-account management
                                     • Text chunking (chunkMode)
                                     • Agent routing / session dispatch
```

The plugin is a **WS Client** that actively connects to the Weibo IM WebSocket endpoint.
Each platform requires a separate plugin containing all protocol details.

### ZeroClaw Bridge: "Universal Bridge Protocol"

```
                                          Weibo adapter (any language)
                                               ↕ [Weibo protocol]
Weibo IM Server ←→ Weibo adapter ←——[Bridge WS]——→ Bridge Channel (WS Server) ←→ Agent
DingTalk Server ←→ DingTalk adapter ←——[Bridge WS]——→      ↑ same instance
Custom system   ←→ Direct connect  ←——[Bridge WS]——→      ↑ same instance
                                                    ↑
                                              Bridge only handles:
                                              • Neutral JSON frame protocol
                                              • Auth + connection management
                                              • sender_id routing
                                              • Draft/typing/approval primitives
```

The Bridge is a **WS Server** that passively accepts adapter connections.
Platform-specific logic lives in external adapters; Bridge knows nothing about any platform.

---

## 3. Dimension-by-Dimension Comparison

### 3.1 Communication Model

| Dimension | openclaw-weibo | ZeroClaw Bridge |
|-----------|---------------|-----------------|
| **WS role** | **Client** (connects to Weibo IM) | **Server** (accepts adapter connections) |
| **Connection target** | `ws://open-im.api.weibo.com/ws/stream` | `ws://localhost:{port}` |
| **Direction** | Agent → Platform | Adapter → Agent |
| **Multiplexing** | 1 connection = 1 Weibo account | 1 connection can multiplex N sender_ids |
| **Heartbeat** | JSON `{"type":"ping"}` / 30s interval | JSON `{"type":"ping"}` / `{"type":"pong"}` |
| **Reconnect** | Built-in exponential backoff (1s→60s) | Adapter responsibility |

Both use WebSocket, but in opposite directions.
The Bridge's server role lets it serve multiple platform adapters simultaneously.

### 3.2 Authentication

| Dimension | openclaw-weibo | ZeroClaw Bridge |
|-----------|---------------|-----------------|
| **Auth flow** | Two-phase: HTTP POST → token, then WS with token in URL params | Single-phase: WS first frame carries token |
| **Token lifecycle** | OAuth token with expiry, auto-refresh with 60s buffer | Static token from config |
| **Token caching** | In-memory Map per accountId, expires at `acquiredAt + expire_in - 60s` | Not needed (static) |
| **Comparison** | Standard string equality | `constant_time_eq` (timing-attack safe) |
| **Auth timeout** | No explicit timeout | 10s auth handshake timeout |

openclaw-weibo's auth is more complex (real OAuth 2.0 flow with Weibo's API).
Bridge's auth is simpler but more secure (constant-time comparison + timeout enforcement).

### 3.3 Message Handling

| Dimension | openclaw-weibo | ZeroClaw Bridge |
|-----------|---------------|-----------------|
| **Dedup** | In-memory Set (max 1000 messageIds, evicts oldest 500) | None (delegated to agent layer) |
| **Text chunking** | Built-in chunker: `length` mode (char slicing) and `newline` mode (SDK helper), default limit 2000/4000 chars | None (adapter/agent responsibility) |
| **Routing** | `resolveAgentRoute()` → sessionKey per (channel, accountId, peer) | `sender_id` → `bridge_{sender_id}` |
| **Multi-account** | Yes — `accounts` config object, each with independent credentials | Single instance, distinguished by sender_id |
| **Frame format** | Weibo IM protocol frames (`type` + `payload`) | Custom JSON frames (`serde(tag = "type")`) |

openclaw-weibo embeds more business logic (dedup, chunking, multi-account) because those are real Weibo requirements.
Bridge stays minimal, pushing these concerns to adapters or the agent layer.

### 3.4 Security Boundaries

| Dimension | openclaw-weibo | ZeroClaw Bridge |
|-----------|---------------|-----------------|
| **Network exposure** | Outbound-only, no listening port | Listening port (default localhost) |
| **Sender filtering** | `allowFrom` whitelist + `dmPolicy` (open/pairing) | `AllowList` (Any / Set, deny-all default) |
| **Connection limit** | N/A (single connection per account) | `max_connections` semaphore (default 64) |
| **Public exposure** | N/A (no listening port) | `allow_public_bind` explicit opt-in |
| **Secret storage** | Config file (`appId` + `appSecret`) | Config file (`token`) |

Different attack surfaces due to architectural choices.
openclaw-weibo as WS Client has no inbound attack surface but holds more sensitive secrets (OAuth credentials).
Bridge exposes a port but has layered defenses (localhost default + AllowList + connection cap + auth timeout).

### 3.5 Feature Coverage

| Feature | openclaw-weibo | ZeroClaw Bridge |
|---------|---------------|-----------------|
| Basic send/receive | Yes | Yes |
| Streaming / progressive response | Yes (dispatcher pattern) | Yes (Draft lifecycle) |
| Typing indicator | Yes (via dispatcher) | Yes (TypingStart/Stop frames) |
| Approval flow | No (requires host support) | Yes (ApprovalPrompt/Response) |
| Reactions | No | Yes (ReactionAdd/Remove) |
| Multi-user multiplexing | Yes (routing + sessionKey) | Yes (sender_id multiplexing) |
| Multi-account | Yes (`accounts` config) | No (single instance) |
| Message dedup | Yes (messageId Set) | No (delegated) |
| Auto-reconnect | Yes (exponential backoff) | No (adapter responsibility) |
| Text chunking | Yes (length/newline modes) | No (adapter responsibility) |
| Graceful shutdown | Yes (AbortSignal) | Yes (connection cleanup + WS close frame) |

### 3.6 Extensibility

| Dimension | openclaw-weibo | ZeroClaw Bridge |
|-----------|---------------|-----------------|
| **New platform cost** | **High**: full plugin needed (auth + protocol + business logic) | **Low**: lightweight adapter only (platform protocol ↔ Bridge JSON) |
| **Code reuse** | Limited (OpenClaw's `core.channel.*` tools help) | Complete (Bridge protocol + server code = zero modification) |
| **Language binding** | Must be TypeScript (OpenClaw plugin system) | Adapter can use any language |
| **Testing** | Must mock each platform's API | Unified WS protocol tests |

---

## 4. Strengths Summary

### openclaw-weibo Strengths

1. **Plug and play**: As an OpenClaw plugin, `npm install` + configure `appId`/`appSecret` → running. No separate adapter process needed.
2. **Deep platform integration**: Direct Weibo IM WS API access — no middleware latency or translation overhead.
3. **Built-in business logic**: Dedup, chunking, multi-account management work out of the box.
4. **Auto-reconnect**: Exponential backoff reconnection is a production-grade essential.
5. **Ecosystem leverage**: OpenClaw's `core.channel.routing`, `core.channel.reply`, `core.channel.text` utilities eliminate boilerplate.
6. **Zero extra processes**: Runs in-process, no external adapter to deploy.

### ZeroClaw Bridge Strengths

1. **"Write once, N platforms"**: Bridge protocol is platform-neutral. Agent core code = O(1). Each new platform adds only a lightweight adapter.
2. **Architecture decoupling**: Platform protocol changes don't touch Agent core. Change blast radius is isolated to external adapters.
3. **Stronger security depth**: constant-time token comparison + auth timeout + public-bind protection + connection cap.
4. **Richer communication primitives**: Draft lifecycle (start/update/finalize/cancel), Approval flow, Reactions — natively supported.
5. **Language freedom**: Adapters can be written in any language, not bound to host runtime.
6. **Performance**: Rust implementation with zero-copy serialization (`OutboundFrame<'a>` uses references). Better memory/CPU under high concurrency.

---

## 5. Design Philosophy Comparison

| Philosophy | openclaw-weibo | ZeroClaw Bridge |
|-----------|---------------|-----------------|
| **Core idea** | **Platform-native adaptation** — one deep-integration plugin per platform | **Protocol abstraction** — one universal bridge + N lightweight adapters |
| **Analogy** | JDBC driver (one native driver per database) | REST API (uniform interface, backend freedom) |
| **Best for** | Deep platform API usage, limited number of platforms | Many/diverse third-party system integrations |
| **Extension model** | O(n) plugins, each 500–1500 lines | O(1) Bridge + O(n) adapters, each 100–300 lines |
| **Coupling** | Tight coupling to platform API (platform changes → plugin changes) | Loose coupling (platform changes → adapter changes only) |

---

## 6. Conclusions

### Why Bridge is the right choice for ZeroClaw

1. ZeroClaw's design goals (high extensibility + high security + high performance) naturally align with the Bridge's universal protocol model.
2. Rust-native implementation has fundamental advantages in concurrency performance and memory efficiency.
3. External adapter model completely isolates platform coupling from the Agent core, consistent with ZeroClaw's trait-driven architecture philosophy.
4. Security design (deny-by-default, constant-time comparison, public-bind protection) meets production-grade standards.

### Designs worth borrowing from openclaw-weibo

| Priority | Improvement | Inspiration |
|----------|-------------|-------------|
| P1 | Provide reference adapter implementations for Weibo/DingTalk/Lark | openclaw-weibo's plug-and-play experience |
| P2 | Add optional `message_id` field to Bridge frame protocol for idempotency/dedup | openclaw-weibo's dedup Set |
| P3 | Document reconnection best practices in Bridge protocol spec | openclaw-weibo's exponential backoff reconnect |
| P3 | Consider extension frame support (`type: "extension"`) | Platform-specific capability passthrough needs |

---

## 7. Reference Files

**openclaw-weibo:**
- `src/channel.ts` — Plugin entry point (ChannelPlugin definition)
- `src/client.ts` — WS Client with reconnect/heartbeat
- `src/bot.ts` — Message handling (dedup, routing, dispatch)
- `src/token.ts` — OAuth token fetch/cache/refresh
- `src/send.ts` — Message sending via WS
- `src/monitor.ts` — Connection lifecycle management
- `src/outbound.ts` — Outbound adapter with text chunker
- `src/policy.ts` — Allowlist matching

**ZeroClaw Bridge:**
- `src/channels/bridge.rs` — Complete Bridge implementation (protocol, auth, routing, tests)
- `src/channels/traits.rs` — Channel trait (send, listen, draft, approval, reaction)
- `src/config/schema.rs` — BridgeConfig struct
