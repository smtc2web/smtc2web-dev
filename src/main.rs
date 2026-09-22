mod framework;
mod media;
mod proxy;
mod server;

use clap::Parser;
use serde::Serialize;
use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::{Arc, LazyLock, Mutex, RwLock};
use std::time::Duration;
use tokio::sync::broadcast;

/* ======================== 日志宏（仅 stderr，不落文件） ======================== */

#[macro_export]
macro_rules! log_debug {
    ($($arg:tt)*) => { eprintln!("[DEBUG] {}", format!($($arg)*)) };
}

#[macro_export]
macro_rules! log_info {
    ($($arg:tt)*) => { eprintln!("[INFO] {}", format!($($arg)*)) };
}

#[macro_export]
macro_rules! log_warn {
    ($($arg:tt)*) => { eprintln!("[WARN] {}", format!($($arg)*)) };
}

#[macro_export]
macro_rules! log_error {
    ($($arg:tt)*) => { eprintln!("[ERROR] {}", format!($($arg)*)) };
}

/* ======================== 媒体数据结构（移植自 smtc2web lib.rs） ======================== */

#[derive(Default, Clone, Serialize, PartialEq)]
pub struct Song {
    pub title: String,
    pub artist: String,
    pub album: String,
    pub album_art: Option<String>,
    pub position: Option<String>,
    pub duration: Option<String>,
    pub pct: Option<f64>,
    pub is_playing: bool,
    pub last_update: u64,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub font_family: String,
}

pub fn format_duration(seconds: u64) -> String {
    let minutes = seconds / 60;
    let secs = seconds % 60;
    format!("{:02}:{:02}", minutes, secs)
}

pub type Shared = Arc<RwLock<Song>>;

pub static CURRENT_APP_ID: LazyLock<Mutex<String>> = LazyLock::new(|| Mutex::new(String::new()));

pub static CURRENT_APP_DISPLAY_NAME: LazyLock<Mutex<String>> =
    LazyLock::new(|| Mutex::new(String::new()));

/* ======================== CLI（严格保留 smtc2web dev 的参数名） ======================== */

#[derive(Parser)]
#[command(
    name = "smtc2web-dev",
    version,
    about = "smtc2web 主题命令行 Web 预览服务器"
)]
struct Cli {
    /// 监听端口
    #[arg(short = 'P', long, default_value_t = 3031)]
    port: u16,

    /// 监听地址
    #[arg(long, default_value = "127.0.0.1")]
    host: String,

    /// 不自动打开浏览器
    #[arg(long)]
    no_open: bool,

    /// 强制使用 Vite 模式（等价于 --framework vite）
    #[arg(long)]
    vite: bool,

    /// 底层框架 dev server 的内部端口
    #[arg(long = "vite-port", default_value_t = 5173)]
    vite_port: u16,

    /// 底层框架：auto / none / vite / next / nuxt / astro / svelte / webpack / vue-cli / angular / script
    #[arg(long, default_value = "auto")]
    framework: String,

    /// 媒体进程过滤规则（默认读取 smtc2web 的 config.toml，再退回 "*"）
    #[arg(long)]
    process_filter: Option<String>,

    /// 主题目录（需包含 theme.toml）
    #[arg(default_value = ".")]
    path: PathBuf,
}

/* ======================== 主题信息 ======================== */

fn parse_theme_info(theme_dir: &Path) -> Option<(String, String, String)> {
    let theme_toml = theme_dir.join("theme.toml");
    let content = std::fs::read_to_string(&theme_toml).ok()?;
    let toml_value: toml::Value = toml::from_str(&content).ok()?;
    let theme_section = toml_value
        .get("smtc2web")
        .and_then(|s| s.get("theme"))
        .unwrap_or(&toml_value);

    Some((
        theme_section
            .get("name")
            .and_then(|v| v.as_str())
            .unwrap_or("Unknown")
            .to_string(),
        theme_section
            .get("version")
            .and_then(|v| v.as_str())
            .unwrap_or("0.0.0")
            .to_string(),
        theme_section
            .get("author")
            .and_then(|v| v.as_str())
            .unwrap_or("Unknown")
            .to_string(),
    ))
}

/// 复用 smtc2web 主程序的进程过滤规则（只读这一个字段，不移植整个 config.rs）
fn config_process_filter() -> Option<String> {
    let path = dirs::config_dir()?.join("smtc2web").join("config.toml");
    let content = std::fs::read_to_string(path).ok()?;
    let value: toml::Value = toml::from_str(&content).ok()?;
    value
        .get("process_filter")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
}

/* ======================== 媒体线程（移植自 dev.rs dev_media_worker） ======================== */

fn media_worker(state: Shared, process_filter: String) {
    #[cfg(target_os = "windows")]
    {
        if let Err(e) = media::smtc::run_event_driven(state, &process_filter) {
            eprintln!("错误: Dev media event worker failed: {}", e);
            log_error!("Dev media event worker failed: {}", e);
        }
    }

    #[cfg(target_os = "linux")]
    {
        let session = match media::PlatformSession::new(&process_filter) {
            Ok(s) => s,
            Err(e) => {
                eprintln!("错误: 创建媒体会话失败: {}", e);
                log_error!("创建媒体会话失败: {}", e);
                return;
            }
        };

        media::poll_media_loop(&session, &state, &CURRENT_APP_ID, &CURRENT_APP_DISPLAY_NAME);
    }

    #[cfg(not(any(target_os = "windows", target_os = "linux")))]
    {
        let _ = (state, process_filter);
    }
}

async fn wait_child(dev: &mut Option<framework::DevServer>) {
    match dev {
        Some(d) => {
            let _ = d.child.wait().await;
        }
        None => std::future::pending::<()>().await,
    }
}

/* ======================== 主入口（移植自 dev.rs run） ======================== */

#[tokio::main]
async fn main() {
    let args = Cli::parse();
    println!("smtc2web-dev - 主题开发服务器");

    // 1. 验证主题目录
    let theme_dir = match std::fs::canonicalize(&args.path) {
        Ok(d) => d,
        Err(e) => {
            eprintln!("错误: 无效的主题目录 '{}': {}", args.path.display(), e);
            std::process::exit(1);
        }
    };
    if !theme_dir.join("theme.toml").exists() {
        eprintln!("错误: 未找到 theme.toml: {}", theme_dir.display());
        std::process::exit(1);
    }

    // 2. 主题信息
    if let Some((name, version, author)) = parse_theme_info(&theme_dir) {
        println!("{} {} v{} by {}", "=".repeat(40), name, version, author);
    }

    // 3. 框架检测
    let launch = if args.vite {
        framework::resolve(&theme_dir, "vite")
    } else {
        framework::resolve(&theme_dir, &args.framework)
    };

    // 4. 媒体
    let state: Shared = Arc::default();
    let process_filter = args
        .process_filter
        .clone()
        .or_else(config_process_filter)
        .unwrap_or_else(|| "*".to_string());
    std::thread::spawn({
        let s = state.clone();
        move || media_worker(s, process_filter)
    });

    // 5. 热重载通道
    let (reload_tx, _) = broadcast::channel::<()>(16);

    // 6. 监听地址
    let address: IpAddr = match args.host.parse() {
        Ok(a) => a,
        Err(_) => {
            eprintln!("警告: 无效的监听地址 '{}', 使用 127.0.0.1", args.host);
            IpAddr::from([127, 0, 0, 1])
        }
    };

    // 7. 框架 dev server
    let mut dev = if let Some(ref launch) = launch {
        println!();
        println!("检测到 {} 项目, 启动 dev server...", launch.name());
        framework::start(&theme_dir, launch, args.vite_port).await
    } else {
        None
    };

    let proxy_port = if let Some(d) = dev.as_mut() {
        framework::wait_ready(d).await;
        if d.child.try_wait().ok().flatten().is_some() {
            eprintln!("警告: 框架 dev server 启动失败, 回退到静态模式");
            None
        } else {
            Some(d.port.clone())
        }
    } else {
        None
    };

    // 8. HTTP 服务器
    let mut server = match server::start(server::ServerConfig {
        address,
        port: args.port,
        theme_dir: theme_dir.clone(),
        state,
        reload_tx: reload_tx.clone(),
        proxy_port: proxy_port.clone(),
    })
    .await
    {
        Ok(s) => s,
        Err(e) => {
            eprintln!("错误: 无法监听 {}:{}: {}", address, args.port, e);
            if let Some(d) = dev.as_mut() {
                let _ = d.child.kill().await;
            }
            std::process::exit(1);
        }
    };

    // 9. 文件监控（静态模式）
    let _watcher = if server.proxy_mode {
        None
    } else {
        server::start_file_watcher(&theme_dir, reload_tx)
    };

    // 10. 信息输出 & 打开浏览器
    let local_addr = format!("{}:{}", address, server.addr.port());
    println!();
    println!("  Dev server: http://{}", local_addr);
    println!("  Theme:      {}", theme_dir.display());
    if let Some(port) = &proxy_port {
        println!(
            "  Framework:  http://localhost:{} (反向代理, 含 WebSocket HMR)",
            port.load(Ordering::Relaxed)
        );
    } else {
        println!("  文件监控已启用");
    }
    println!();

    if !args.no_open
        && let Err(e) = open::that(format!("http://{}", local_addr))
    {
        eprintln!("警告: 打开浏览器失败: {}", e);
        log_warn!("打开浏览器失败: {}", e);
    }

    // 11. 等待退出
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            println!();
            println!("收到退出信号, 正在关闭...");
            log_info!("收到退出信号, 正在关闭...");
        }
        _ = wait_child(&mut dev) => {
            println!();
            println!("框架进程已退出, 正在关闭...");
            log_info!("框架进程已退出, 正在关闭...");
        }
    }

    // 12. 清理
    if let Some(d) = dev.as_mut() {
        let _ = d.child.kill().await;
    }
    server.shutdown();
    let handle = server.handle;
    let _ = tokio::time::timeout(Duration::from_secs(3), handle).await;
    println!("开发服务器已关闭");
    log_info!("开发服务器已关闭");
}
