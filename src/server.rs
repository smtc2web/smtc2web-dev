use crate::Shared;
use crate::proxy;
use futures_util::StreamExt;
use http_body_util::combinators::BoxBody;
use http_body_util::{BodyExt, Full, StreamBody};
use hyper::body::{Bytes, Frame, Incoming};
use hyper::header::CONTENT_TYPE;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use notify::{
    Config as NotifyConfig, Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher,
};
use std::convert::Infallible;
use std::net::{IpAddr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU16, Ordering};
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::{broadcast, oneshot};
use tokio_stream::wrappers::{BroadcastStream, IntervalStream};

pub type BoxError = Box<dyn std::error::Error + Send + Sync>;
pub type Body = BoxBody<Bytes, BoxError>;

pub fn body_full(data: impl Into<Bytes>) -> Body {
    Full::new(data.into())
        .map_err(|_: Infallible| -> BoxError { unreachable!() })
        .boxed()
}

/* ---------- SSE 热重载脚本（移植自 dev.rs） ---------- */
const SSE_RELOAD_SCRIPT: &str = r#"<script>
(function(){var e=new EventSource('/__dev_reload');e.addEventListener('reload',function(){e.close();location.reload()});e.onerror=function(){e.close()}})();
</script>"#;

const BODY_CLOSE_TAG: &str = "</body>";

/* ==================== 服务器 ==================== */

pub struct ServerConfig {
    pub address: IpAddr,
    pub port: u16,
    pub theme_dir: PathBuf,
    pub state: Shared,
    pub reload_tx: broadcast::Sender<()>,
    /// Some => 框架模式，全部请求（除 /api/now）反向代理到该端口
    pub proxy_port: Option<Arc<AtomicU16>>,
}

pub struct Server {
    pub addr: SocketAddr,
    pub handle: tokio::task::JoinHandle<()>,
    pub proxy_mode: bool,
    shutdown_tx: Option<oneshot::Sender<()>>,
}

impl Server {
    pub fn shutdown(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
    }
}

struct AppState {
    theme_dir: PathBuf,
    state: Shared,
    reload_tx: broadcast::Sender<()>,
    proxy_port: Option<Arc<AtomicU16>>,
}

pub async fn start(cfg: ServerConfig) -> std::io::Result<Server> {
    let listener = TcpListener::bind(SocketAddr::new(cfg.address, cfg.port)).await?;
    let addr = listener.local_addr()?;
    let proxy_mode = cfg.proxy_port.is_some();
    let app = Arc::new(AppState {
        theme_dir: cfg.theme_dir,
        state: cfg.state,
        reload_tx: cfg.reload_tx,
        proxy_port: cfg.proxy_port,
    });

    let (shutdown_tx, mut shutdown_rx) = oneshot::channel::<()>();
    let handle = tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = &mut shutdown_rx => break,
                accepted = listener.accept() => {
                    let Ok((stream, _peer)) = accepted else { break };
                    let app = app.clone();
                    tokio::spawn(async move {
                        let io = TokioIo::new(stream);
                        let service = service_fn(move |req| handle_request(req, app.clone()));
                        let conn = hyper::server::conn::http1::Builder::new()
                            .serve_connection(io, service)
                            .with_upgrades();
                        let _ = conn.await;
                    });
                }
            }
        }
    });

    Ok(Server {
        addr,
        handle,
        proxy_mode,
        shutdown_tx: Some(shutdown_tx),
    })
}

async fn handle_request(
    req: Request<Incoming>,
    app: Arc<AppState>,
) -> Result<Response<Body>, Infallible> {
    let uri_path = req.uri().path();
    let path = uri_path.to_string();

    if path == "/api/now" {
        let data = {
            let song = app.state.read().unwrap();
            serde_json::to_vec(&*song).unwrap_or_else(|_| b"{}".to_vec())
        };
        return Ok(respond(StatusCode::OK, "application/json", data));
    }

    if path == "/__dev_reload" && app.proxy_port.is_none() {
        return Ok(sse_response(&app.reload_tx));
    }

    if let Some(port) = &app.proxy_port {
        let port = port.load(Ordering::Relaxed);
        return Ok(match proxy::forward(req, port).await {
            Ok(resp) => resp,
            Err(e) => respond(
                StatusCode::BAD_GATEWAY,
                "text/plain; charset=utf-8",
                format!("上游 dev server 不可用 (localhost:{}): {}", port, e),
            ),
        });
    }

    Ok(serve_static(uri_path.trim_start_matches('/'), &app.theme_dir).await)
}

fn respond(status: StatusCode, content_type: &str, data: impl Into<Bytes>) -> Response<Body> {
    Response::builder()
        .status(status)
        .header(CONTENT_TYPE, content_type)
        .body(body_full(data))
        .unwrap()
}

fn sse_response(reload_tx: &broadcast::Sender<()>) -> Response<Body> {
    let reloads = BroadcastStream::new(reload_tx.subscribe()).map(|_| ());
    let ticks = IntervalStream::new(tokio::time::interval(Duration::from_secs(15))).map(|_| ());
    let stream = futures_util::stream::select(reloads, ticks)
        .map(|_| Ok::<_, BoxError>(Frame::data(Bytes::from_static(b"data: reload\n\n"))));
    Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, "text/event-stream")
        .header("cache-control", "no-cache")
        .body(BodyExt::boxed(StreamBody::new(stream)))
        .unwrap()
}

/* ---------- 静态模式：文件服务（移植自 dev.rs） ---------- */

async fn serve_static(path: &str, theme_dir: &Path) -> Response<Body> {
    let path = if path.is_empty() { "index.html" } else { path };
    let file_path = theme_dir.join(path);

    let Ok(canonical_base) = tokio::fs::canonicalize(theme_dir).await else {
        return not_found();
    };
    let Ok(resolved_path) = tokio::fs::canonicalize(&file_path).await else {
        return not_found();
    };

    if !resolved_path.starts_with(&canonical_base) {
        return not_found();
    }

    let Ok(data) = tokio::fs::read(&resolved_path).await else {
        return not_found();
    };

    let mime = mime_guess::from_path(path)
        .first_or_octet_stream()
        .to_string();

    let body = if mime.starts_with("text/html") {
        inject_sse(&String::from_utf8_lossy(&data)).into_bytes()
    } else {
        data
    };

    respond(StatusCode::OK, &mime, body)
}

fn not_found() -> Response<Body> {
    respond(
        StatusCode::NOT_FOUND,
        "text/plain; charset=utf-8",
        "Not Found",
    )
}

fn inject_sse(html: &str) -> String {
    if let Some(pos) = html.to_lowercase().rfind(BODY_CLOSE_TAG) {
        let mut s = html.to_string();
        s.insert_str(pos, SSE_RELOAD_SCRIPT);
        s
    } else {
        format!("{}{}", html, SSE_RELOAD_SCRIPT)
    }
}

/* ---------- 文件监控（移植自 dev.rs） ---------- */

pub fn start_file_watcher(
    theme_dir: &Path,
    reload_tx: broadcast::Sender<()>,
) -> Option<RecommendedWatcher> {
    let theme_dir = theme_dir.to_path_buf();
    let (tx, rx) = std::sync::mpsc::channel::<notify::Result<Event>>();

    let mut watcher = match RecommendedWatcher::new(
        move |res| {
            let _ = tx.send(res);
        },
        NotifyConfig::default(),
    ) {
        Ok(w) => w,
        Err(e) => {
            eprintln!("警告: 文件监控初始化失败: {}", e);
            return None;
        }
    };

    if let Err(e) = watcher.watch(&theme_dir, RecursiveMode::Recursive) {
        eprintln!("警告: 文件监控启动失败: {}", e);
        return None;
    }

    std::thread::spawn(move || {
        let mut pending = false;
        loop {
            match rx.recv_timeout(Duration::from_millis(100)) {
                Ok(Ok(event))
                    if matches!(
                        event.kind,
                        EventKind::Modify(_) | EventKind::Create(_) | EventKind::Remove(_)
                    ) =>
                {
                    pending = true;
                    while let Ok(Ok(_)) = rx.try_recv() {}
                    if pending {
                        let _ = reload_tx.send(());
                        pending = false;
                    }
                }
                Ok(Err(_)) | Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                    if pending {
                        let _ = reload_tx.send(());
                        pending = false;
                    }
                }
                Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
                _ => {}
            }
        }
    });

    Some(watcher)
}

/* ==================== 测试 ==================== */

#[cfg(test)]
mod tests {
    use super::*;
    use http_body_util::Full;
    use hyper::client::conn::http1 as client_http1;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    fn temp_theme(name: &str) -> PathBuf {
        let dir =
            std::env::temp_dir().join(format!("smtc2web-dev-test-{}-{}", std::process::id(), name));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(
            dir.join("theme.toml"),
            "[smtc2web.theme]\nname = \"test\"\n",
        )
        .unwrap();
        std::fs::write(dir.join("index.html"), "<html><body>hi</body></html>").unwrap();
        dir
    }

    async fn start_test_server(theme_dir: PathBuf, proxy_port: Option<u16>) -> Server {
        start(ServerConfig {
            address: IpAddr::from([127, 0, 0, 1]),
            port: 0,
            theme_dir,
            state: Arc::new(std::sync::RwLock::new(crate::Song::default())),
            reload_tx: broadcast::channel(16).0,
            proxy_port: proxy_port.map(|p| Arc::new(AtomicU16::new(p))),
        })
        .await
        .unwrap()
    }

    async fn send(addr: SocketAddr, method: &str, uri: &str, body: &str) -> (StatusCode, String) {
        let stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        let (mut sender, conn) = client_http1::handshake(TokioIo::new(stream)).await.unwrap();
        tokio::spawn(async move {
            let _ = conn.await;
        });
        let req = Request::builder()
            .method(method)
            .uri(uri)
            .header("content-type", "text/plain")
            .body(Full::new(Bytes::from(body.to_string())))
            .unwrap();
        let resp = sender.send_request(req).await.unwrap();
        let status = resp.status();
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        (status, String::from_utf8_lossy(&bytes).to_string())
    }

    #[tokio::test]
    async fn static_mode_serves_html_with_reload_and_api() {
        let dir = temp_theme("static");
        let mut server = start_test_server(dir.clone(), None).await;
        let addr = server.addr;

        let (status, body) = send(addr, "GET", "/", "").await;
        assert_eq!(status, StatusCode::OK);
        assert!(body.contains("__dev_reload"), "SSE 脚本未注入: {}", body);

        let (status, body) = send(addr, "GET", "/api/now", "").await;
        assert_eq!(status, StatusCode::OK);
        assert!(body.contains("\"is_playing\""), "意外的 /api/now: {}", body);

        let (status, _) = send(addr, "GET", "/../Cargo.toml", "").await;
        assert_ne!(status, StatusCode::OK, "路径穿越未被拦截");

        server.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn proxy_mode_forwards_http() {
        let upstream = spawn_upstream().await;
        let dir = temp_theme("proxy");
        let mut server = start_test_server(dir.clone(), Some(upstream)).await;
        let addr = server.addr;

        let (status, body) = send(addr, "GET", "/foo", "").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, "GET:");

        let (status, body) = send(addr, "POST", "/bar", "hi").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, "POST:hi");

        server.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn proxy_mode_tunnels_websocket_upgrade() {
        let upstream = spawn_upstream().await;
        let dir = temp_theme("ws");
        let mut server = start_test_server(dir.clone(), Some(upstream)).await;
        let addr = server.addr;

        let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        let req = format!(
            "GET /hmr HTTP/1.1\r\nHost: 127.0.0.1:{}\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n\r\n",
            addr.port()
        );
        stream.write_all(req.as_bytes()).await.unwrap();

        let mut headers = Vec::new();
        let mut byte = [0u8; 1];
        while !headers.ends_with(b"\r\n\r\n") {
            stream.read_exact(&mut byte).await.unwrap();
            headers.push(byte[0]);
        }
        let headers = String::from_utf8_lossy(&headers);
        assert!(
            headers.starts_with("HTTP/1.1 101"),
            "未透传 101: {}",
            headers
        );

        stream.write_all(b"ping").await.unwrap();
        let mut echoed = [0u8; 4];
        tokio::time::timeout(Duration::from_secs(3), stream.read_exact(&mut echoed))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(&echoed, b"ping");

        server.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// 桩上游：普通请求回显 "METHOD:body"，Upgrade 请求回 101 并 echo 4 字节
    async fn spawn_upstream() -> u16 {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    break;
                };
                tokio::spawn(async move {
                    let io = TokioIo::new(stream);
                    let service = service_fn(|mut req: Request<Incoming>| async move {
                        if req.headers().get("upgrade").is_some() {
                            let on_upgrade = hyper::upgrade::on(&mut req);
                            tokio::spawn(async move {
                                if let Ok(upgraded) = on_upgrade.await {
                                    let mut io = TokioIo::new(upgraded);
                                    let mut buf = [0u8; 4];
                                    if let Ok(n) = io.read(&mut buf).await {
                                        let _ = io.write_all(&buf[..n]).await;
                                    }
                                }
                            });
                            return Ok::<_, Infallible>(
                                Response::builder()
                                    .status(StatusCode::SWITCHING_PROTOCOLS)
                                    .header("connection", "upgrade")
                                    .header("upgrade", "websocket")
                                    .body(body_full(Bytes::new()))
                                    .unwrap(),
                            );
                        }
                        let method = req.method().clone();
                        let body = req.into_body().collect().await.unwrap().to_bytes();
                        Ok(Response::new(body_full(format!(
                            "{}:{}",
                            method,
                            String::from_utf8_lossy(&body)
                        ))))
                    });
                    let conn = hyper::server::conn::http1::Builder::new()
                        .serve_connection(io, service)
                        .with_upgrades();
                    let _ = conn.await;
                });
            }
        });
        port
    }
}
