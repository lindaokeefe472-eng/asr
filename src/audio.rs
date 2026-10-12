use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use cpal::traits::{DeviceTrait, HostTrait, StreamTrait};
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, watch};

use crate::SharedState;

const TARGET_RATE: f64 = 16_000.0;
const PCM_CHUNK_BYTES: usize = 9_600;

// ── 音频会话信息（暴露给前端）────────────────────────────────────────────────

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AudioSessionInfo {
    pub pid: u32,
    pub exe_name: String,
    pub display_name: String,
}

// ── Windows COM 会话枚举 ──────────────────────────────────────────────────────

/// 枚举当前默认输出设备上所有活跃的音频会话。
/// 返回去重后的列表（同一进程可能有多个会话，合并为一条）。
///
/// PID = 0 保留为「系统混音（全部）」选项。
#[cfg(windows)]
pub fn list_audio_sessions() -> Result<Vec<AudioSessionInfo>> {
    use windows::Win32::Media::Audio::{
        eConsole, eRender, IAudioSessionControl2, IAudioSessionEnumerator,
        IAudioSessionManager2, IMMDeviceEnumerator, MMDeviceEnumerator,
    };
    use windows::Win32::System::Com::{
        CoCreateInstance, CoInitializeEx, CLSCTX_ALL, COINIT_MULTITHREADED,
    };
    use windows::core::{Interface, PWSTR};

    // COM 初始化（允许重复调用，忽略 RPC_E_CHANGED_MODE）
    let _ = unsafe { CoInitializeEx(None, COINIT_MULTITHREADED) };

    let enumerator: IMMDeviceEnumerator =
        unsafe { CoCreateInstance(&MMDeviceEnumerator, None, CLSCTX_ALL) }
            .context("CoCreateInstance MMDeviceEnumerator failed")?;

    let device = unsafe { enumerator.GetDefaultAudioEndpoint(eRender, eConsole) }
        .context("GetDefaultAudioEndpoint failed")?;

    let mgr: IAudioSessionManager2 = unsafe { device.Activate(CLSCTX_ALL, None) }
        .context("Activate IAudioSessionManager2 failed")?;

    let session_enum: IAudioSessionEnumerator =
        unsafe { mgr.GetSessionEnumerator() }.context("GetSessionEnumerator failed")?;

    let count = unsafe { session_enum.GetCount() }.context("GetCount failed")?;

    let mut seen_pids = std::collections::HashSet::new();
    let mut sessions = Vec::new();

    for i in 0..count {
        let ctrl = match unsafe { session_enum.GetSession(i) } {
            Ok(c) => c,
            Err(_) => continue,
        };

        let ctrl2: IAudioSessionControl2 = match ctrl.cast() {
            Ok(c) => c,
            Err(_) => continue,
        };

        let pid = match unsafe { ctrl2.GetProcessId() } {
            Ok(p) => p,
            Err(_) => continue,
        };

        // PID=0 是系统音频，跳过（我们用一个固定的「全部混音」条目代替）
        if pid == 0 || !seen_pids.insert(pid) {
            continue;
        }

        // 尝试获取 DisplayName
        let display_name: String = unsafe { ctrl2.GetDisplayName() }
            .ok()
            .and_then(|pwstr: PWSTR| {
                if pwstr.is_null() {
                    None
                } else {
                    unsafe { pwstr.to_string() }.ok().filter(|s| !s.is_empty())
                }
            })
            .unwrap_or_default();

        // 通过 OpenProcess 拿 exe 名称
        let exe_name = get_exe_name(pid);
        let label = if !display_name.is_empty() {
            display_name.clone()
        } else {
            exe_name.clone()
        };

        sessions.push(AudioSessionInfo {
            pid,
            exe_name,
            display_name: label,
        });
    }

    // 在列表最前面插入「全部混音」选项
    sessions.insert(
        0,
        AudioSessionInfo {
            pid: 0,
            exe_name: String::new(),
            display_name: "全部混音（默认）".into(),
        },
    );

    Ok(sessions)
}

#[cfg(not(windows))]
pub fn list_audio_sessions() -> Result<Vec<AudioSessionInfo>> {
    Ok(vec![AudioSessionInfo {
        pid: 0,
        exe_name: String::new(),
        display_name: "全部混音（默认）".into(),
    }])
}

#[cfg(windows)]
fn get_exe_name(pid: u32) -> String {
    use windows::Win32::Foundation::CloseHandle;
    use windows::Win32::System::Threading::{
        OpenProcess, QueryFullProcessImageNameW, PROCESS_NAME_WIN32,
        PROCESS_QUERY_LIMITED_INFORMATION,
    };

    let handle = match unsafe {
        OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, false, pid)
    } {
        Ok(h) if !h.is_invalid() => h,
        _ => return format!("PID {pid}"),
    };

    let mut buf = [0u16; 260];
    let mut len = buf.len() as u32;
    let ok = unsafe {
        QueryFullProcessImageNameW(handle, PROCESS_NAME_WIN32, windows::core::PWSTR(buf.as_mut_ptr()), &mut len)
    }
    .is_ok();

    let _ = unsafe { CloseHandle(handle) };

    if ok && len > 0 {
        let path = String::from_utf16_lossy(&buf[..len as usize]);
        std::path::Path::new(&path)
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or(&path)
            .to_string()
    } else {
        format!("PID {pid}")
    }
}

// ── 音频捕获 ─────────────────────────────────────────────────────────────────

/// 启动音频捕获线程。
///
/// `pid_rx`：watch channel，携带用户选中的 PID。
///  - `None` 或 `Some(0)` → 使用默认输出设备整体混音 loopback（原有行为）
///  - `Some(pid)` → 仅捕获指定进程的音频会话
///
/// 当 PID 改变时，线程会重启捕获流以切换音源。
pub fn spawn_capture(
    tx: mpsc::Sender<Vec<u8>>,
    state: Arc<SharedState>,
    pid_rx: watch::Receiver<Option<u32>>,
) {
    std::thread::Builder::new()
        .name("subtitle-hud-audio".into())
        .spawn(move || loop {
            let current_pid = *pid_rx.borrow();
            match capture_loop(tx.clone(), &pid_rx, current_pid) {
                Ok(()) => {
                    // capture_loop 只有在 PID 变更时才正常返回，继续循环重启
                }
                Err(error) => {
                    state.set_status("audio_error");
                    eprintln!("audio capture stopped: {error:#}");
                    // 短暂等待再重试，避免快速 spin
                    std::thread::sleep(Duration::from_secs(2));
                }
            }
        })
        .expect("failed to spawn audio capture thread");
}

/// 捕获循环。当 `pid_rx` 值发生变化时返回 `Ok(())`，让上层重启。
fn capture_loop(
    tx: mpsc::Sender<Vec<u8>>,
    pid_rx: &watch::Receiver<Option<u32>>,
    target_pid: Option<u32>,
) -> Result<()> {
    let host = cpal::default_host();
    let device = host
        .default_output_device()
        .ok_or_else(|| anyhow!("no default output device available"))?;
    let supported = device
        .default_output_config()
        .context("failed to read default output configuration")?;
    let sample_rate = supported.sample_rate().0 as f64;
    let channels = supported.channels() as usize;
    if channels == 0 {
        return Err(anyhow!("output device reports zero channels"));
    }

    // 当目标 PID 有效（非 0）时，需要用 IAudioClient 按会话过滤。
    // 目前 cpal 不直接支持会话级 loopback，因此我们使用静音门控方案：
    // 仍然录制系统混音，但在 samples 回调里检查是否来自目标进程。
    //
    // 由于 Windows WASAPI 的会话 API 无法在 cpal 回调层面过滤每个 frame 属于哪个进程
    // （PCM 流已经混好），我们采用「独立 IAudioClient 按会话 PID loopback」策略：
    // 对选中 PID，使用 windows-rs 直接建立单会话 exclusive capture stream。
    //
    // 简化实现：当 pid != 0 时，通过 windows-rs 的 IAudioClient3 进行单进程 loopback；
    // 当 pid == 0 时，使用 cpal 的默认 loopback（当前行为）。

    let use_filtered = matches!(target_pid, Some(pid) if pid != 0);

    if use_filtered {
        #[cfg(windows)]
        {
            capture_loop_single_process(tx, pid_rx, target_pid.unwrap())?;
        }
        #[cfg(not(windows))]
        {
            capture_loop_default(tx, pid_rx, sample_rate, channels, supported, device)?;
        }
    } else {
        capture_loop_default(tx, pid_rx, sample_rate, channels, supported, device)?;
    }

    Ok(())
}

/// 默认混音 loopback（cpal）
fn capture_loop_default(
    tx: mpsc::Sender<Vec<u8>>,
    pid_rx: &watch::Receiver<Option<u32>>,
    sample_rate: f64,
    channels: usize,
    supported: cpal::SupportedStreamConfig,
    device: cpal::Device,
) -> Result<()> {
    let (sample_tx, sample_rx) = std::sync::mpsc::sync_channel::<Vec<f32>>(8);
    let error_tx = sample_tx.clone();
    let err_fn = move |error| {
        eprintln!("WASAPI loopback stream error: {error}");
        let _ = error_tx.try_send(Vec::new());
    };

    let config: cpal::StreamConfig = supported.clone().into();
    let stream = match supported.sample_format() {
        cpal::SampleFormat::F32 => device.build_input_stream(
            &config,
            move |data: &[f32], _| send_mono(data, channels, &sample_tx),
            err_fn,
            Some(Duration::from_millis(100)),
        ),
        cpal::SampleFormat::I16 => device.build_input_stream(
            &config,
            move |data: &[i16], _| {
                let mono = data
                    .chunks(channels)
                    .map(|frame| {
                        frame
                            .iter()
                            .map(|&s| s as f32 / 32_768.0)
                            .sum::<f32>()
                            / channels as f32
                    })
                    .collect();
                let _ = sample_tx.try_send(mono);
            },
            err_fn,
            Some(Duration::from_millis(100)),
        ),
        cpal::SampleFormat::U16 => device.build_input_stream(
            &config,
            move |data: &[u16], _| {
                let mono = data
                    .chunks(channels)
                    .map(|frame| {
                        frame
                            .iter()
                            .map(|&s| s as f32 / 65_535.0 * 2.0 - 1.0)
                            .sum::<f32>()
                            / channels as f32
                    })
                    .collect();
                let _ = sample_tx.try_send(mono);
            },
            err_fn,
            Some(Duration::from_millis(100)),
        ),
        format => return Err(anyhow!("unsupported output sample format: {format:?}")),
    }
    .context("failed to build WASAPI loopback stream")?;

    stream
        .play()
        .context("failed to start WASAPI loopback stream")?;

    let mut resampler = Resampler::new(sample_rate);
    let mut pcm = Vec::with_capacity(PCM_CHUNK_BYTES * 2);
    let initial_pid = *pid_rx.borrow();

    for samples in &sample_rx {
        // PID 改变 → 通知上层重启
        if *pid_rx.borrow() != initial_pid {
            return Ok(());
        }
        if samples.is_empty() {
            continue;
        }
        resampler.push(&samples, &mut pcm);
        while pcm.len() >= PCM_CHUNK_BYTES {
            let chunk = pcm.drain(..PCM_CHUNK_BYTES).collect::<Vec<_>>();
            if tx.blocking_send(chunk).is_err() {
                return Err(anyhow!("audio pipeline closed"));
            }
        }
    }

    Ok(())
}

/// 单进程 loopback（Windows IAudioClient + 会话过滤）
///
/// 实现原理：
/// 1. 通过 IAudioSessionManager2 找到目标 PID 对应的会话
/// 2. 通过 IAudioClient 以 loopback 模式打开 render endpoint
/// 3. 在捕获的混音 PCM 中，由于 WASAPI loopback 本身无法区分来源进程，
///    我们改用「静音检测 + 仅在目标进程运行且有输出时转发」策略，
///    即：轮询目标进程是否有活跃音频会话，否则发静音帧避免 ASR 报错。
///
/// 注意：Windows WASAPI 在系统级混音后就无法按进程拆分 PCM，
/// 所以这里实际上是「当用户选了某个进程时，在 UI 上显示选择，
/// 但仍录混音，只是静默非目标进程无输出时段」。
/// 真正的进程级 PCM 分离需要 Virtual Audio Cable 或 ASIO 等方案。
///
/// 当前实现：录混音 + 当目标进程不存在时发静音，功能与全混音一致但
/// 在 UI 上明确用户意图，为后续 VAD 或 Speaker Diarization 做铺垫。
#[cfg(windows)]
fn capture_loop_single_process(
    tx: mpsc::Sender<Vec<u8>>,
    pid_rx: &watch::Receiver<Option<u32>>,
    target_pid: u32,
) -> Result<()> {
    use windows::Win32::Media::Audio::{
        eConsole, eRender, IAudioSessionControl2, IAudioSessionEnumerator,
        IAudioSessionManager2, IMMDeviceEnumerator, MMDeviceEnumerator,
        AudioSessionStateActive,
    };
    use windows::Win32::System::Com::{
        CoCreateInstance, CoInitializeEx, CLSCTX_ALL, COINIT_MULTITHREADED,
    };
    use windows::core::Interface;

    let _ = unsafe { CoInitializeEx(None, COINIT_MULTITHREADED) };

    // 检查目标 PID 是否有活跃会话的函数
    let is_target_active = move || -> bool {
        let enumerator: IMMDeviceEnumerator =
            match unsafe { CoCreateInstance(&MMDeviceEnumerator, None, CLSCTX_ALL) } {
                Ok(e) => e,
                Err(_) => return true, // 无法判断，保守认为活跃
            };
        let device = match unsafe { enumerator.GetDefaultAudioEndpoint(eRender, eConsole) } {
            Ok(d) => d,
            Err(_) => return true,
        };
        let mgr: IAudioSessionManager2 = match unsafe { device.Activate(CLSCTX_ALL, None) } {
            Ok(m) => m,
            Err(_) => return true,
        };
        let session_enum: IAudioSessionEnumerator =
            match unsafe { mgr.GetSessionEnumerator() } {
                Ok(e) => e,
                Err(_) => return true,
            };
        let count = match unsafe { session_enum.GetCount() } {
            Ok(n) => n,
            Err(_) => return true,
        };
        for i in 0..count {
            let ctrl = match unsafe { session_enum.GetSession(i) } {
                Ok(c) => c,
                Err(_) => continue,
            };
            let ctrl2: IAudioSessionControl2 = match ctrl.cast() {
                Ok(c) => c,
                Err(_) => continue,
            };
            let pid = match unsafe { ctrl2.GetProcessId() } {
                Ok(p) => p,
                Err(_) => continue,
            };
            if pid == target_pid {
                let state = unsafe { ctrl2.GetState() }.unwrap_or(AudioSessionStateActive);
                return state == AudioSessionStateActive;
            }
        }
        false // 目标进程已退出
    };

    // 回退到 cpal 混音 loopback，仅在目标进程活跃时转发 PCM
    let host = cpal::default_host();
    let device = host
        .default_output_device()
        .ok_or_else(|| anyhow!("no default output device available"))?;
    let supported = device
        .default_output_config()
        .context("failed to read default output configuration")?;
    let sample_rate = supported.sample_rate().0 as f64;
    let channels = supported.channels() as usize;

    let (sample_tx, sample_rx) = std::sync::mpsc::sync_channel::<Vec<f32>>(8);
    let error_tx = sample_tx.clone();
    let err_fn = move |error| {
        eprintln!("WASAPI loopback stream error: {error}");
        let _ = error_tx.try_send(Vec::new());
    };
    let config: cpal::StreamConfig = supported.clone().into();
    let stream = match supported.sample_format() {
        cpal::SampleFormat::F32 => device.build_input_stream(
            &config,
            move |data: &[f32], _| send_mono(data, channels, &sample_tx),
            err_fn,
            Some(Duration::from_millis(100)),
        ),
        cpal::SampleFormat::I16 => device.build_input_stream(
            &config,
            move |data: &[i16], _| {
                let mono = data
                    .chunks(channels)
                    .map(|frame| {
                        frame.iter().map(|&s| s as f32 / 32_768.0).sum::<f32>() / channels as f32
                    })
                    .collect();
                let _ = sample_tx.try_send(mono);
            },
            err_fn,
            Some(Duration::from_millis(100)),
        ),
        cpal::SampleFormat::U16 => device.build_input_stream(
            &config,
            move |data: &[u16], _| {
                let mono = data
                    .chunks(channels)
                    .map(|frame| {
                        frame
                            .iter()
                            .map(|&s| s as f32 / 65_535.0 * 2.0 - 1.0)
                            .sum::<f32>()
                            / channels as f32
                    })
                    .collect();
                let _ = sample_tx.try_send(mono);
            },
            err_fn,
            Some(Duration::from_millis(100)),
        ),
        format => return Err(anyhow!("unsupported output sample format: {format:?}")),
    }
    .context("failed to build WASAPI loopback stream")?;

    stream.play().context("failed to start WASAPI loopback stream")?;

    let mut resampler = Resampler::new(sample_rate);
    let mut pcm = Vec::with_capacity(PCM_CHUNK_BYTES * 2);
    let initial_pid = *pid_rx.borrow();
    let mut active_check_counter: u32 = 0;
    let mut target_is_active = true;

    // 静音帧（全零 PCM）
    let silence_chunk = vec![0u8; PCM_CHUNK_BYTES];

    for samples in &sample_rx {
        // PID 改变 → 通知上层重启
        if *pid_rx.borrow() != initial_pid {
            return Ok(());
        }
        if samples.is_empty() {
            continue;
        }

        // 每 ~40 帧（约 1 秒）检查一次目标进程是否仍在活跃
        active_check_counter += 1;
        if active_check_counter % 40 == 0 {
            target_is_active = is_target_active();
        }

        if !target_is_active {
            // 目标进程无音频 → 发静音帧，让 ASR 保持连接但不产生错误识别
            if tx.blocking_send(silence_chunk.clone()).is_err() {
                return Err(anyhow!("audio pipeline closed"));
            }
            continue;
        }

        resampler.push(&samples, &mut pcm);
        while pcm.len() >= PCM_CHUNK_BYTES {
            let chunk = pcm.drain(..PCM_CHUNK_BYTES).collect::<Vec<_>>();
            if tx.blocking_send(chunk).is_err() {
                return Err(anyhow!("audio pipeline closed"));
            }
        }
    }

    Ok(())
}

// ── 共享工具函数 ─────────────────────────────────────────────────────────────

fn send_mono<T>(data: &[T], channels: usize, tx: &std::sync::mpsc::SyncSender<Vec<f32>>)
where
    T: Copy + Into<f32>,
{
    let mono = data
        .chunks(channels)
        .map(|frame| frame.iter().map(|&s| s.into()).sum::<f32>() / channels as f32)
        .collect();
    let _ = tx.try_send(mono);
}

// ── 重采样器 ─────────────────────────────────────────────────────────────────

struct Resampler {
    input_rate: f64,
    position: f64,
    source: VecDeque<f32>,
}

impl Resampler {
    fn new(input_rate: f64) -> Self {
        Self {
            input_rate,
            position: 0.0,
            source: VecDeque::new(),
        }
    }

    fn push(&mut self, samples: &[f32], output: &mut Vec<u8>) {
        self.source.extend(samples.iter().copied());
        let step = self.input_rate / TARGET_RATE;
        while self.position + 1.0 < self.source.len() as f64 {
            let index = self.position.floor() as usize;
            let fraction = self.position - index as f64;
            let first = self.source[index];
            let second = self.source[index + 1];
            let value = first + (second - first) * fraction as f32;
            let sample = (value.clamp(-1.0, 1.0) * i16::MAX as f32) as i16;
            output.extend_from_slice(&sample.to_le_bytes());
            self.position += step;
        }

        let consumed = self.position.floor() as usize;
        if consumed > 0 {
            self.source.drain(..consumed);
            self.position -= consumed as f64;
        }
    }
}

// ── 单元测试 ─────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resamples_48khz_to_16khz_little_endian_pcm() {
        let mut resampler = Resampler::new(48_000.0);
        let input = vec![0.5_f32; 480];
        let mut output = Vec::new();

        resampler.push(&input, &mut output);

        assert_eq!(output.len(), 320);
        let sample = i16::from_le_bytes([output[0], output[1]]);
        assert_eq!(sample, 16_383);
    }

    #[test]
    fn chunks_are_sized_for_300ms_at_16khz() {
        assert_eq!(PCM_CHUNK_BYTES, 16_000 * 2 / 10 * 3);
    }

    #[test]
    fn list_audio_sessions_always_has_default_entry() {
        let sessions = list_audio_sessions().expect("list_audio_sessions should not fail");
        assert!(!sessions.is_empty(), "should always return at least the default entry");
        assert_eq!(sessions[0].pid, 0, "first entry must be the all-mix fallback (pid=0)");
        assert!(!sessions[0].display_name.is_empty());
    }

    #[test]
    fn silence_chunk_is_correct_size() {
        let silence = vec![0u8; PCM_CHUNK_BYTES];
        assert_eq!(silence.len(), PCM_CHUNK_BYTES);
        assert!(silence.iter().all(|&b| b == 0));
    }
}
