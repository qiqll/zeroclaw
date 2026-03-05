修改总结

  新增功能：Bridge Channel（第三方系统桥接通道）

  背景

  ZeroClaw 拥有 20+ 内置 channel（Telegram、Discord、Slack 等），但缺少通用的第三方系统接入机制。现有的 webhook 接口是无状态的，WebSocket /ws/chat 仅提供会话级历史，无法获得 channel 模式的完整体验。

  Bridge Channel 通过运行一个 WebSocket 服务端，让任何第三方系统作为 WS 客户端连接后即成为一个完整的 ZeroClaw channel，享受全部 channel 能力。

  变更清单

  ┌────────────────────────┬──────┬───────────────────────────────────────────────────────────────────────────┐
  │          文件          │ 操作 │                                   说明                                    │
  ├────────────────────────┼──────┼───────────────────────────────────────────────────────────────────────────┤
  │ src/channels/bridge.rs │ 新建 │ BridgeChannel 核心实现（~890 行），含 Channel trait 全方法实现 + 单元测试 │
  ├────────────────────────┼──────┼───────────────────────────────────────────────────────────────────────────┤
  │ src/channels/mod.rs    │ 修改 │ 模块声明、re-export、工厂注册（+15 行）                                   │
  ├────────────────────────┼──────┼───────────────────────────────────────────────────────────────────────────┤
  │ src/config/schema.rs   │ 修改 │ BridgeConfig 结构体、ChannelsConfig 集成、测试修复（+41 行）              │
  └────────────────────────┴──────┴───────────────────────────────────────────────────────────────────────────┘

  零新依赖 — 所有使用的 crate 均已在 Cargo.toml 中（tokio-tungstenite、futures-util、uuid、serde_json）。

  核心能力

  - Token 认证 — 连接后首条消息必须是 auth 握手，使用 constant-time 比较防止时序攻击
  - 多用户复用 — 单个 WS 连接通过 sender_id 字段区分不同终端用户，支持第三方系统作为中间层
  - Per-sender 对话历史 — 按 bridge_{sender_id} 隔离，与其他 channel 行为一致
  - Typing 指示器 — typing_start / typing_stop 帧
  - Draft 流式更新 — draft_start / draft_update / draft_finalize / draft_cancel 生命周期
  - 结构化工具审批 — JSON 格式的 approval_prompt + approval_response
  - Emoji 反应 — reaction_add / reaction_remove
  - 心跳 — ping / pong
  - 安全默认值 — 绑定 127.0.0.1、sender allowlist deny-by-default

  配置方法

  在 config.toml 中添加：

  [channels.bridge]
  host = "127.0.0.1"        # 默认值，可省略
  port = 9090                # 必填，WS 监听端口
  token = "your-secret-token" # 必填，客户端认证 token
  allowed_senders = ["*"]    # 空=拒绝所有，["*"]=允许所有，["id1","id2"]=白名单
  stream_mode = "partial"    # "off"(默认) 或 "partial"(启用 draft 流式更新)

  协议说明

  所有消息为 JSON 文本帧，通过 "type" 字段区分消息类型。

  连接流程：

  客户端                                        ZeroClaw
    │                                              │
    │──── WebSocket 连接 ──────────────────────────▶│
    │                                              │
    │──── {"type":"auth","token":"xxx"} ──────────▶│
    │                                              │
    │◀──── {"type":"auth_result","success":true} ──│
    │                                              │
    │──── {"type":"message","content":"你好",       │
    │      "sender_id":"user_001"} ───────────────▶│
    │                                              │
    │◀──── {"type":"typing_start",                 │
    │       "sender_id":"user_001"} ───────────────│
    │                                              │
    │◀──── {"type":"message","content":"你好！",    │
    │       "sender_id":"user_001"} ───────────────│

  Inbound（客户端 → ZeroClaw）：

  ┌───────────────────┬─────────────────────────────────┬────────────────────────────┐
  │       type        │              字段               │            说明            │
  ├───────────────────┼─────────────────────────────────┼────────────────────────────┤
  │ auth              │ token                           │ 认证握手（必须是首条消息） │
  ├───────────────────┼─────────────────────────────────┼────────────────────────────┤
  │ message           │ content, sender_id, thread_id?  │ 用户消息                   │
  ├───────────────────┼─────────────────────────────────┼────────────────────────────┤
  │ approval_response │ request_id, approved, sender_id │ 工具审批响应               │
  ├───────────────────┼─────────────────────────────────┼────────────────────────────┤
  │ ping              │ —                               │ 心跳                       │
  └───────────────────┴─────────────────────────────────┴────────────────────────────┘

  Outbound（ZeroClaw → 客户端）：

  ┌────────────────────────────────────────────────────────────┬─────────────────────────────────────────────┬───────────────┐
  │                            type                            │                    字段                     │     说明      │
  ├────────────────────────────────────────────────────────────┼─────────────────────────────────────────────┼───────────────┤
  │ auth_result                                                │ success, error?                             │ 认证结果      │
  ├────────────────────────────────────────────────────────────┼─────────────────────────────────────────────┼───────────────┤
  │ message                                                    │ content, sender_id, thread_ts?              │ Agent 回复    │
  ├────────────────────────────────────────────────────────────┼─────────────────────────────────────────────┼───────────────┤
  │ typing_start / typing_stop                                 │ sender_id                                   │ Typing 指示器 │
  ├────────────────────────────────────────────────────────────┼─────────────────────────────────────────────┼───────────────┤
  │ draft_start / draft_update / draft_finalize / draft_cancel │ draft_id, content?, sender_id               │ 流式 draft    │
  ├────────────────────────────────────────────────────────────┼─────────────────────────────────────────────┼───────────────┤
  │ approval_prompt                                            │ request_id, tool_name, arguments, sender_id │ 工具审批请求  │
  ├────────────────────────────────────────────────────────────┼─────────────────────────────────────────────┼───────────────┤
  │ reaction_add / reaction_remove                             │ channel_id, message_id, emoji               │ Emoji 反应    │
  ├────────────────────────────────────────────────────────────┼─────────────────────────────────────────────┼───────────────┤
  │ error                                                      │ message                                     │ 错误通知      │
  ├────────────────────────────────────────────────────────────┼─────────────────────────────────────────────┼───────────────┤
  │ pong                                                       │ —                                           │ 心跳响应      │
  └────────────────────────────────────────────────────────────┴─────────────────────────────────────────────┴───────────────┘

  验证方法

  # 编译
  cargo build

  # 格式 + lint
  cargo fmt --all -- --check
  cargo clippy --all-targets -- -D warnings

  # 运行 bridge 模块测试
  cargo test --lib channels::bridge

  # 运行全量测试
  cargo test

  # 手动集成测试（使用 websocat 或任意 WS 客户端）
  websocat ws://127.0.0.1:9090
  # 发送: {"type":"auth","token":"your-secret-token"}
  # 接收: {"type":"auth_result","success":true}
  # 发送: {"type":"message","content":"Hello!","sender_id":"test_user"}

  不包含的内容（YAGNI）

  - Client 模式（ZeroClaw 主动连接第三方 WS server）
  - TLS/WSS（通过反向代理处理）
  - 多 bridge 实例
  - Binary/protobuf 协议
  - Web UI / 管理界面


codex代码审查：下面是基于当前代码的实现正确性评审（按严重程度排序，含文件与行号引用）。

__高严重__

- `src/config/schema.rs:5961` / `src/config/schema.rs:6123`：桥接通道 `BridgeConfig.token` 未被纳入 `decrypt_channel_secrets` / `encrypt_channel_secrets` 的加解密流程。其它渠道的敏感字段都会经过 SecretStore 处理，但 `bridge.token` 未被处理，导致在启用 `config.secrets.encrypt` 时该 token 仍以明文保存在配置里，存在严重安全回归风险。建议在两个函数内加入对 `channels.bridge.token` 的 decrypt/encrypt 调用（与其它渠道一致的 field_name 命名）。
- `src/config/schema.rs:5525` / `src/config/schema.rs:5532`：`BridgeConfig` 明确声明 token 是认证所需字段，但未被 SecretStore 保护（结合上条），造成 token 泄露风险在落盘配置/日志场景中更高。

__中严重__

- `src/channels/bridge.rs:322` / `src/channels/bridge.rs:326`：`supports_draft_updates` 仅在 `StreamMode::Partial` 时返回 true，但 `StreamMode::Full` 理论上也支持流式/草稿更新（若语义为“支持渐进式更新”）。目前实现会把 Full 视为不支持，导致上层可能绕开 draft 更新。若 `Full` 的语义是“完整输出一次”则此处合理，但需要确认。建议：如果 Full 也期望增量更新，应改为 `StreamMode::Partial | StreamMode::Full`；否则在注释/文档中说明 Full 不支持 draft。
- `src/channels/bridge.rs:570` / `src/channels/bridge.rs:599`：`sender_routing` 在收到 `InboundFrame::Message` 时才更新，这意味着只要客户端尚未发过 `Message`，服务端就无法向该 sender 发送（例如审批提示、工具调用提示等）。同时 `ApprovalResponse` 路径不会更新 routing。若客户端只发送审批响应但未发普通消息，后续发送到该 sender 的消息可能找不到路由。建议在 `ApprovalResponse` 路径也更新 routing，或在 Auth 握手后允许客户端提供 sender 绑定列表。

__低严重 / 风险提示__

- `src/channels/bridge.rs:599` / `src/channels/bridge.rs:620`：审批回应通过伪造 `/approve-allow`/`/approve-deny` 指令发送到审批系统，逻辑依赖现有命令解析器。若未来命令前缀或审批机制变动，此路径可能悄然失效。建议加测试覆盖或抽一个内部 API（例如 `ApprovalManager::resolve_by_id`）以避免依赖命令字符串。
- `src/channels/bridge.rs:587` / `src/channels/bridge.rs:620`：`sender` 统一格式为 `bridge_{sender_id}`，而 `reply_target` 为原始 `sender_id`。如果外部系统 sender_id 与 CLI 用户名/其他通道用户冲突，虽然 channel 名为 `bridge` 但审批与非 CLI 策略里可能依赖 sender 过滤规则，请确认这里的 sender 前缀策略与 `ApprovalManager::is_non_cli_approval_actor_allowed` 的规则一致（可能需要 bridge 专用 allowlist 或明确 channel+sender 组合）。

__确认无明显问题的点（已检查）__

- `src/channels/mod.rs:17` / `src/channels/mod.rs:4849`：桥接通道已正确注册与启动，传入 host/port/token/allowed_senders/stream_mode。
- `src/config/schema.rs:3996` / `src/config/schema.rs:4005`：`channels_except_webhook` 已纳入 bridge（启用统计正常）。
- `src/channels/bridge.rs:434` / `src/channels/bridge.rs:484`：握手流程要求客户端先发送 Auth，认证失败则立即返回 AuthResult(false) 并断开，流程正常。
- `src/channels/bridge.rs:535` / `src/channels/bridge.rs:563`：消息解析错误只反馈 Error 帧并保持连接，符合容错预期。
- `src/channels/bridge.rs:573` / `src/channels/bridge.rs:584`：Message → ChannelMessage 映射合理，按 `bridge_{sender_id}` 维护会话历史，线程字段 `thread_id` 直通。

__建议的最小修复__

1. 将 `BridgeConfig.token` 加入 `decrypt_channel_secrets` / `encrypt_channel_secrets` 处理。
2. 明确 `StreamMode::Full` 是否支持 draft 更新，必要时调整 `supports_draft_updates`。
3. 在 `ApprovalResponse` 路径更新 sender 路由，避免只有审批响应时无法回推消息。
