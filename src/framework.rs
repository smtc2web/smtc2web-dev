use crate::{log_error, log_info, log_warn};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::sync::atomic::{AtomicU16, Ordering};
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::process::{Child, Command};

/* ---------- 框架表：检测文件 + 启动命令 ---------- */
/* 命令形如 `<bin> <args...> <port_flag> <port> [strict_port_flag]` */

pub struct Framework {
    pub id: &'static str,
    pub detect: &'static [&'static str],
    pub bin: &'static str,
    pub args: &'static [&'static str],
    pub port_flag: &'static str,
    pub strict_port_flag: Option<&'static str>,
}

const VITE: Framework = Framework {
    id: "vite",
    detect: &[
        "vite.config.js",
        "vite.config.mjs",
        "vite.config.ts",
        "vite.config.mts",
        "vite.config.cjs",
        "vite.config.cts",
    ],
    bin: "vite",
    args: &[],
    port_flag: "--port",
    strict_port_flag: Some("--strictPort"),
};

const FRAMEWORKS: &[Framework] = &[
    Framework {
        id: "next",
        detect: &[
            "next.config.js",
            "next.config.mjs",
            "next.config.ts",
            "next.config.cjs",
        ],
        bin: "next",
        args: &["dev"],
        port_flag: "-p",
        strict_port_flag: None,
    },
    Framework {
        id: "nuxt",
        detect: &["nuxt.config.js", "nuxt.config.ts", "nuxt.config.mjs"],
        bin: "nuxt",
        args: &["dev"],
        port_flag: "--port",
        strict_port_flag: None,
    },
    Framework {
        id: "astro",
        detect: &["astro.config.js", "astro.config.ts", "astro.config.mjs"],
        bin: "astro",
        args: &["dev"],
        port_flag: "--port",
        strict_port_flag: None,
    },
    Framework {
        id: "svelte",
        detect: &["svelte.config.js", "svelte.config.ts"],
        bin: "vite",
        args: &[],
        port_flag: "--port",
        strict_port_flag: Some("--strictPort"),
    },
    VITE,
    Framework {
        id: "vue-cli",
        detect: &["vue.config.js", "vue.config.ts"],
        bin: "vue-cli-service",
        args: &["serve"],
        port_flag: "--port",
        strict_port_flag: None,
    },
    Framework {
        id: "webpack",
        detect: &[
            "webpack.config.js",
            "webpack.config.ts",
            "webpack.config.mjs",
        ],
        bin: "webpack",
        args: &["serve"],
        port_flag: "--port",
        strict_port_flag: None,
    },
    Framework {
        id: "angular",
        detect: &["angular.json"],
        bin: "ng",
        args: &["serve"],
        port_flag: "--port",
        strict_port_flag: None,
    },
];

/* ---------- 启动方式 ---------- */

pub enum Launch {
    Tool(&'static Framework),
    Script { script: String },
}

impl Launch {
    pub fn name(&self) -> String {
        match self {
            Launch::Tool(fw) => fw.id.to_string(),
            Launch::Script { script } => format!("package.json script \"{}\"", script),
        }
    }
}

pub struct DevServer {
    pub child: Child,
    pub port: Arc<AtomicU16>,
}

/* ---------- 检测 ---------- */

pub fn resolve(dir: &Path, forced: &str) -> Option<Launch> {
    if forced == "none" {
        return None;
    }

    if forced == "auto" {
        for fw in FRAMEWORKS {
            if fw.detect.iter().any(|f| dir.join(f).exists()) {
                return Some(Launch::Tool(fw));
            }
        }
        if package_has_dep(dir, "vite") {
            return Some(Launch::Tool(&VITE));
        }
        return detect_script(dir).map(|script| Launch::Script { script });
    }

    if forced == "script" {
        return detect_script(dir).map(|script| Launch::Script { script });
    }

    if let Some(fw) = FRAMEWORKS.iter().find(|f| f.id == forced) {
        return Some(Launch::Tool(fw));
    }

    eprintln!("警告: 未知框架 '{}', 使用静态模式", forced);
    None
}

fn package_has_dep(dir: &Path, name: &str) -> bool {
    let Ok(content) = std::fs::read_to_string(dir.join("package.json")) else {
        return false;
    };
    let Ok(json) = serde_json::from_str::<serde_json::Value>(&content) else {
        return false;
    };
    ["dependencies", "devDependencies"]
        .iter()
        .any(|key| json.get(key).and_then(|v| v.get(name)).is_some())
}

fn detect_script(dir: &Path) -> Option<String> {
    let content = std::fs::read_to_string(dir.join("package.json")).ok()?;
    let json: serde_json::Value = serde_json::from_str(&content).ok()?;
    let scripts = json.get("scripts")?;
    ["dev", "start", "serve"]
        .iter()
        .find(|name| scripts.get(*name).is_some())
        .map(|name| name.to_string())
}

/* ---------- 启动 ---------- */

pub async fn start(dir: &Path, launch: &Launch, inner_port: u16) -> Option<DevServer> {
    let port = Arc::new(AtomicU16::new(inner_port));
    let child = match launch {
        Launch::Tool(fw) => spawn_tool(dir, fw, inner_port).await,
        Launch::Script { script } => spawn_script(dir, script, inner_port, port.clone()).await,
    }?;
    Some(DevServer { child, port })
}

async fn spawn_tool(dir: &Path, fw: &Framework, inner_port: u16) -> Option<Child> {
    let mut args: Vec<String> = fw.args.iter().map(|s| s.to_string()).collect();
    args.push(fw.port_flag.to_string());
    args.push(inner_port.to_string());
    if let Some(strict) = fw.strict_port_flag {
        args.push(strict.to_string());
    }

    // 优先本地 node_modules/.bin，避免 Windows 上找不到 npx.cmd / 每次走 npx 下载
    let (program, full_args) = match local_bin(dir, fw.bin) {
        Some(path) => (path.to_string_lossy().to_string(), args),
        None => {
            let npx = if cfg!(windows) { "npx.cmd" } else { "npx" };
            let mut full = vec![fw.bin.to_string()];
            full.extend(args);
            (npx.to_string(), full)
        }
    };

    spawn(
        dir,
        &program,
        &full_args,
        None,
        Stdio::inherit(),
        Stdio::inherit(),
    )
}

async fn spawn_script(
    dir: &Path,
    script: &str,
    inner_port: u16,
    port: Arc<AtomicU16>,
) -> Option<Child> {
    let npm = if cfg!(windows) { "npm.cmd" } else { "npm" };
    let child = spawn(
        dir,
        npm,
        &["run".to_string(), script.to_string()],
        Some(inner_port),
        Stdio::piped(),
        Stdio::inherit(),
    )?;

    // 透传 stdout 并嗅探真实端口
    let mut child = child;
    if let Some(stdout) = child.stdout.take() {
        use tokio::io::{AsyncBufReadExt, BufReader};
        tokio::spawn(async move {
            let mut lines = BufReader::new(stdout).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                println!("{}", line);
                if let Some(p) = parse_port(&line) {
                    port.store(p, Ordering::Relaxed);
                }
            }
        });
    }
    Some(child)
}

fn spawn(
    dir: &Path,
    program: &str,
    args: &[String],
    port_env: Option<u16>,
    stdout: Stdio,
    stderr: Stdio,
) -> Option<Child> {
    let mut cmd = Command::new(program);
    cmd.args(args)
        .current_dir(dir)
        .stdin(Stdio::null())
        .stdout(stdout)
        .stderr(stderr);
    if let Some(port) = port_env {
        cmd.env("PORT", port.to_string());
    }

    match cmd.spawn() {
        Ok(child) => {
            eprintln!("启动: {} {}", program, args.join(" "));
            Some(child)
        }
        Err(e) => {
            eprintln!("错误: 启动 {} 失败: {}", program, e);
            log_error!("启动 {} 失败: {}", program, e);
            None
        }
    }
}

fn local_bin(dir: &Path, bin: &str) -> Option<PathBuf> {
    let base = dir.join("node_modules").join(".bin").join(bin);
    if cfg!(windows) {
        let cmd = base.with_extension("cmd");
        if cmd.exists() {
            return Some(cmd);
        }
    }
    if base.exists() { Some(base) } else { None }
}

fn parse_port(line: &str) -> Option<u16> {
    let (idx, scheme_len) = match (line.find("http://"), line.find("https://")) {
        (Some(i), Some(j)) => (i.min(j), if i < j { 7 } else { 8 }),
        (Some(i), None) => (i, 7),
        (None, Some(j)) => (j, 8),
        (None, None) => return None,
    };
    let rest = &line[idx + scheme_len..];
    let authority = rest.split(['/', ' ', '\r', '\n']).next()?;
    let (_, port) = authority.rsplit_once(':')?;
    port.parse().ok()
}

/* ---------- 就绪探测 ---------- */

pub async fn wait_ready(dev: &mut DevServer) {
    for _ in 0..30 {
        if let Ok(Some(status)) = dev.child.try_wait() {
            eprintln!("警告: 框架 dev server 已退出 (exit: {:?})", status.code());
            log_warn!("框架 dev server 已退出 (exit: {:?})", status.code());
            return;
        }
        let port = dev.port.load(Ordering::Relaxed);
        // 用 localhost 解析：Vite 等框架默认可能只监听 ::1
        if TcpStream::connect(("localhost", port)).await.is_ok() {
            let url = format!("http://localhost:{}", port);
            println!("框架 dev server 就绪: {}", url);
            log_info!("框架 dev server 就绪: {}", url);
            return;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    eprintln!("警告: 框架 dev server 启动超时");
    log_warn!("框架 dev server 启动超时");
}
