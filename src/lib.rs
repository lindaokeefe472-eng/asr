mod audio;
use audio::AudioSessionInfo;

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::Result;
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use tauri::{LogicalSize, Manager, State, WebviewWindow};
use tauri_plugin_global_shortcut::{Builder as GlobalShortcutBuilder, Code, ShortcutState};
use tokio::sync::{mpsc, watch};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use url::Url;

// ── 翻译 API 常量 ─────────────────────────────────────────────────────────────
const TRANSLATE_API_URL: &str = "https://index-translate.bilibili.com/v1/chat/completions";
const TRANSLATE_MODEL: &str = "Index-Translate-35B-A3B";
/// 翻译目标语言（可按需修改：中文 / 英文 / 日文 等）
const TRANSLATE_TARGET_LANG: &str = "中文";

const DEFAULT_SERVER_URL: &str = "ws://192.168.10.2:8765/v1/audio/stream";

fn configured_server_url() -> String {
    std::env::var("SUBTITLE_HUD_SERVER_URL").unwrap_or_else(|_| DEFAULT_SERVER_URL.into())
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Subtitle {
    #[serde(alias = "text", alias = "transcript", alias = "result")]
    text: String,
    #[serde(default)]
    translated: String,
    #[serde(default = "default_language")]
    language: String,
    #[serde(default)]
    latency_ms: Option<u64>,
}

fn default_language() -> String {
    "auto".into()
}

impl Default for Subtitle {
    fn default() -> Self {
        Self {
            text: "等待原文…".into(),
            translated: "等待翻译…".into(),
            language: "auto".into(),
            latency_ms: None,
        }
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct Snapshot {
    server_url: String,
    status: String,
    subtitle: Subtitle,
    passthrough: bool,
    selected_pid: Option<u32>,
    translation_enabled: bool,
    language: String,
}

pub struct SharedState {
    server_url: Mutex<String>,
    status: Mutex<String>,
    subtitle: Mutex<Subtitle>,
    url_tx: watch::Sender<String>,
    passthrough: AtomicBool,
    /// 当前选中的音频进程 PID。None / Some(0) 表示使用全局混音。
    selected_pid: Mutex<Option<u32>>,
    pid_tx: watch::Sender<Option<u32>>,
    /// 当前字幕版本号，用于防止异步翻译乱序覆盖
    subtitle_version: AtomicU64,
    /// 是否开启大模型翻译。关闭时只接收并显示 ASR 识别结果（超低延迟调试模式）
    translation_enabled: AtomicBool,
    /// ASR 识别的目标语种（如 "en", "ja", "zh", "auto" 等）
    language: Mutex<String>,
    language_tx: watch::Sender<String>,
}

impl SharedState {
    fn new(
        url_tx: watch::Sender<String>,
        pid_tx: watch::Sender<Option<u32>>,
        language_tx: watch::Sender<String>,
    ) -> Self {
        Self {
            server_url: Mutex::new(configured_server_url()),
            status: Mutex::new("connecting".into()),
            subtitle: Mutex::new(Subtitle::default()),
            url_tx,
            passthrough: AtomicBool::new(false),
            selected_pid: Mutex::new(None),
            pid_tx,
            subtitle_version: AtomicU64::new(0),
            translation_enabled: AtomicBool::new(true),
            language: Mutex::new("en".into()),
            language_tx,
        }
    }

    fn set_status(&self, status: &str) {
        if let Ok(mut value) = self.status.lock() {
            *value = status.into();
        }
    }

    /// 第一阶段：收到 ASR 文本后 0ms 即刻更新原文，同时保留当前的翻译内容（绝不跳成“翻译中…”，平滑过渡），并返回新版本号
    fn update_source_text(&self, mut new_sub: Subtitle) -> u64 {
        let version = self.subtitle_version.fetch_add(1, Ordering::SeqCst) + 1;
        if let Ok(mut current) = self.subtitle.lock() {
            // 如果新接收的结构里没有译文，保留当前正在显示的译文，避免屏幕反复跳动闪烁
            if new_sub.translated.is_empty() {
                new_sub.translated = current.translated.clone();
            }
            *current = new_sub;
        }
        version
    }

    /// 第二阶段：大模型异步翻译完成后局部填充译文（若版本号匹配，则采纳；若已过时，则丢弃）
    fn update_translated_text(&self, version: u64, translated: String) {
        let current_version = self.subtitle_version.load(Ordering::SeqCst);
        if current_version == version {
            if let Ok(mut current) = self.subtitle.lock() {
                current.translated = translated;
            }
        }
    }

    fn snapshot(&self) -> Snapshot {
        Snapshot {
            server_url: self
                .server_url
                .lock()
                .map(|value| value.clone())
                .unwrap_or_default(),
            status: self
                .status
                .lock()
                .map(|value| value.clone())
                .unwrap_or_else(|_| "offline".into()),
            subtitle: self
                .subtitle
                .lock()
                .map(|value| value.clone())
                .unwrap_or_default(),
            passthrough: self.passthrough.load(Ordering::Relaxed),
            selected_pid: self
                .selected_pid
                .lock()
                .map(|v| *v)
                .unwrap_or(None),
            translation_enabled: self.translation_enabled.load(Ordering::Relaxed),
            language: self
                .language
                .lock()
                .map(|v| v.clone())
                .unwrap_or_else(|_| "en".into()),
        }
    }
}

#[tauri::command]
fn get_snapshot(state: State<'_, Arc<SharedState>>) -> Snapshot {
    state.snapshot()
}

#[tauri::command]
fn set_server_url(url: String, state: State<'_, Arc<SharedState>>) -> Result<(), String> {
    let parsed = Url::parse(url.trim()).map_err(|_| "WebSocket 地址格式无效".to_string())?;
    if !matches!(parsed.scheme(), "ws" | "wss") || parsed.host_str().is_none() {
        return Err("地址必须使用 ws:// 或 wss://，并包含主机名".into());
    }
    let normalized = parsed.to_string();
    *state
        .server_url
        .lock()
        .map_err(|_| "状态不可用".to_string())? = normalized.clone();
    state
        .url_tx
        .send(normalized)
        .map_err(|_| "后台连接任务已停止".to_string())?;
    Ok(())
}

#[tauri::command]
fn start_dragging(window: WebviewWindow) -> Result<(), String> {
    window.start_dragging().map_err(|error| error.to_string())
}

#[tauri::command]
fn close_window(window: WebviewWindow) -> Result<(), String> {
    window.close().map_err(|error| error.to_string())
}

#[tauri::command]
fn set_passthrough(
    enabled: bool,
    window: WebviewWindow,
    state: State<'_, Arc<SharedState>>,
) -> Result<(), String> {
    window
        .set_ignore_cursor_events(enabled)
        .map_err(|error| error.to_string())?;
    state.passthrough.store(enabled, Ordering::Relaxed);
    Ok(())
}

#[tauri::command]
fn resize_window(width: f64, height: f64, window: WebviewWindow) -> Result<(), String> {
    let width = width.clamp(220.0, 900.0);
    let height = height.clamp(88.0, 180.0);
    window
        .set_size(tauri::Size::Logical(LogicalSize { width, height }))
        .map_err(|error| error.to_string())
}

/// 枚举当前默认输出设备上所有活跃的音频会话，供前端渲染选择列表。
#[tauri::command]
fn list_audio_sessions() -> Result<Vec<AudioSessionInfo>, String> {
    audio::list_audio_sessions().map_err(|e| e.to_string())
}

/// 设置要捕获的目标进程 PID。pid = None 或 0 表示恢复全局混音。
#[tauri::command]
fn set_audio_session(
    pid: Option<u32>,
    state: State<'_, Arc<SharedState>>,
) -> Result<(), String> {
    let normalized = pid.filter(|&p| p != 0);
    *state
        .selected_pid
        .lock()
        .map_err(|_| "状态不可用".to_string())? = normalized;
    state
        .pid_tx
        .send(normalized)
        .map_err(|_| "音频线程已停止".to_string())?;
    Ok(())
}

/// 设置要识别的目标语种，并通知后端实时生效
#[tauri::command]
fn set_language(
    language: String,
    state: State<'_, Arc<SharedState>>,
) -> Result<(), String> {
    let lang = language.trim().to_lowercase();
    if lang.is_empty() {
        return Err("语种不能为空".into());
    }
    *state
        .language
        .lock()
        .map_err(|_| "状态不可用".to_string())? = lang.clone();
    state
        .language_tx
        .send(lang)
        .map_err(|_| "WebSocket 任务已停止".to_string())?;
    Ok(())
}

/// 开启或关闭大模型翻译。关闭后将只显示纯 ASR 识别结果（最低延迟）
#[tauri::command]
fn set_translation_enabled(
    enabled: bool,
    state: State<'_, Arc<SharedState>>,
) -> Result<(), String> {
    state.translation_enabled.store(enabled, Ordering::Relaxed);
    Ok(())
}

async fn websocket_loop(
    state: Arc<SharedState>,
    mut audio_rx: mpsc::Receiver<Vec<u8>>,
    mut url_rx: watch::Receiver<String>,
    mut language_rx: watch::Receiver<String>,
    client: reqwest::Client,
) {
    loop {
        let url = url_rx.borrow().clone();
        state.set_status("connecting");
        match connect_async(&url).await {
            Ok((mut socket, _response)) => {
                state.set_status("connected");
                let result =
                    run_connection(&state, &mut socket, &mut audio_rx, &mut url_rx, &mut language_rx, &url, &client).await;
                if result.is_err() {
                    state.set_status("reconnecting");
                }
            }
            Err(error) => {
                eprintln!("WebSocket connection failed: {error}");
                state.set_status("reconnecting");
            }
        }

        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs(2)) => {},
            changed = url_rx.changed() => {
                if changed.is_err() { return; }
            }
        }
    }
}

async fn run_connection(
    state: &Arc<SharedState>,
    socket: &mut tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    audio_rx: &mut mpsc::Receiver<Vec<u8>>,
    url_rx: &mut watch::Receiver<String>,
    language_rx: &mut watch::Receiver<String>,
    connected_url: &str,
    client: &reqwest::Client,
) -> Result<()> {
    // 建立连接后，直接发送一条 JSON 文本消息告知后端正在听的语种
    let initial_lang = language_rx.borrow().clone();
    let set_lang_msg = serde_json::json!({
        "action": "set_language",
        "language": initial_lang
    })
    .to_string();
    if let Err(e) = socket.send(Message::Text(set_lang_msg.into())).await {
        eprintln!("failed to send initial set_language message: {e}");
    }

    loop {
        tokio::select! {
            Some(pcm) = audio_rx.recv() => {
                socket.send(Message::Binary(pcm.into())).await?;
            }
            Some(message) = socket.next() => {
                match message? {
                    Message::Text(text) => update_subtitle(state, client, text.as_ref()),
                    Message::Binary(data) => update_subtitle(state, client, std::str::from_utf8(&data).unwrap_or_default()),
                    Message::Ping(payload) => socket.send(Message::Pong(payload)).await?,
                    Message::Close(_) => return Err(anyhow::anyhow!("server closed connection")),
                    _ => {}
                }
            }
            changed = language_rx.changed() => {
                if changed.is_ok() {
                    let new_lang = language_rx.borrow().clone();
                    let msg = serde_json::json!({
                        "action": "set_language",
                        "language": new_lang
                    }).to_string();
                    let _ = socket.send(Message::Text(msg.into())).await;
                }
            }
            changed = url_rx.changed() => {
                if changed.is_err() || url_rx.borrow().as_str() != connected_url {
                    return Err(anyhow::anyhow!("server URL changed"));
                }
            }
            else => return Err(anyhow::anyhow!("connection ended")),
        }
    }
}

// ── 翻译 API 请求 / 响应结构体 ─────────────────────────────────────────────────
#[derive(Serialize)]
struct TranslateRequest<'a> {
    model: &'a str,
    messages: Vec<TranslateMessage<'a>>,
}

#[derive(Serialize)]
struct TranslateMessage<'a> {
    role: &'a str,
    content: String,
}

#[derive(Deserialize)]
struct TranslateResponse {
    choices: Vec<TranslateChoice>,
}

#[derive(Deserialize)]
struct TranslateChoice {
    message: TranslateChoiceMessage,
}

#[derive(Deserialize)]
struct TranslateChoiceMessage {
    content: String,
}

/// 调用 bilibili Index-Translate API 翻译文本，失败时返回原文
async fn translate_text(client: &reqwest::Client, source: &str) -> String {
    if source.is_empty() {
        return source.to_string();
    }
    let body = TranslateRequest {
        model: TRANSLATE_MODEL,
        messages: vec![TranslateMessage {
            role: "user",
            content: format!(
                "请将以下内容翻译为{lang}，只输出译文，不要解释：\n\n{text}",
                lang = TRANSLATE_TARGET_LANG,
                text = source,
            ),
        }],
    };
    match client.post(TRANSLATE_API_URL).json(&body).send().await {
        Ok(resp) => match resp.json::<TranslateResponse>().await {
            Ok(parsed) => parsed
                .choices
                .into_iter()
                .next()
                .map(|c| c.message.content.trim().to_string())
                .unwrap_or_else(|| source.to_string()),
            Err(err) => {
                eprintln!("[translate] 解析响应失败: {err}");
                source.to_string()
            }
        },
        Err(err) => {
            eprintln!("[translate] 请求失败: {err}");
            source.to_string()
        }
    }
}

/// 解析 ASR 消息：
/// 1. 0ms 立即更新原文（界面显示「翻译中…」），保证原文零延迟上屏，且绝不阻塞 WebSocket 事件循环。
/// 2. 异步在后台并发触发翻译任务，并使用自增版本号（version）防止网络延迟导致的乱序覆盖。
/// 3. 若用户设置只接收 ASR，则跳过大模型请求，译文直接同步为原文或提示语。
fn update_subtitle(state: &Arc<SharedState>, client: &reqwest::Client, text: &str) {
    let Ok(mut subtitle) = serde_json::from_str::<Subtitle>(text) else {
        return;
    };

    let source = subtitle.text.clone();
    let is_translate_enabled = state.translation_enabled.load(Ordering::Relaxed);

    if !is_translate_enabled {
        // 纯 ASR 模式：不调用大模型，翻译行直接同步原文，双行统一显示最纯粹的 ASR 结果
        subtitle.translated = source.clone();
        state.update_source_text(subtitle);
        return;
    }

    // 开启翻译模式：0ms 立即刷新原文，保留上一句的译文，绝不显示“翻译中…”占位符
    let version = state.update_source_text(subtitle);

    // 在后台异步任务中请求大模型翻译，完全不阻塞当前 WebSocket 接收与录音管道
    let state_clone = state.clone();
    let client_clone = client.clone();
    tauri::async_runtime::spawn(async move {
        let translated = translate_text(&client_clone, &source).await;
        state_clone.update_translated_text(version, translated);
    });
}


#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    let initial_url = configured_server_url();
    let (url_tx, url_rx) = watch::channel(initial_url);
    let (pid_tx, pid_rx) = watch::channel::<Option<u32>>(None);
    let (language_tx, language_rx) = watch::channel("en".to_string());
    let state = Arc::new(SharedState::new(url_tx, pid_tx, language_tx));
    let (audio_tx, audio_rx) = mpsc::channel(8);
    let shortcut_state = state.clone();
    let shortcut_window_label = "main".to_string();

    audio::spawn_capture(audio_tx, state.clone(), pid_rx);
    // 创建可复用的 HTTP 客户端（带 5 秒超时），用于翻译 API 请求
    let http_client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("failed to build HTTP client");
    tauri::async_runtime::spawn(websocket_loop(state.clone(), audio_rx, url_rx, language_rx, http_client));


    tauri::Builder::default()
        .plugin(
            GlobalShortcutBuilder::new()
                .with_shortcut("F8")
                .expect("failed to register F8 recovery shortcut")
                .with_handler(move |app, shortcut, event| {
                    if shortcut.key == Code::F8 && event.state() == ShortcutState::Pressed {
                        if let Some(window) = app.get_webview_window(&shortcut_window_label) {
                            let _ = window.set_ignore_cursor_events(false);
                            shortcut_state.passthrough.store(false, Ordering::Relaxed);
                        }
                    }
                })
                .build(),
        )
        .manage(state)
        .invoke_handler(tauri::generate_handler![
            get_snapshot,
            set_server_url,
            start_dragging,
            close_window,
            set_passthrough,
            resize_window,
            list_audio_sessions,
            set_audio_session,
            set_translation_enabled,
            set_language,
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
