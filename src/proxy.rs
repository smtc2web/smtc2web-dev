use crate::server::{Body, BoxError};
use http_body_util::BodyExt;
use hyper::body::Incoming;
use hyper::header::HeaderValue;
use hyper::header::{HOST, ORIGIN, TRANSFER_ENCODING};
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use tokio::net::TcpStream;

/// 把请求转发到框架 dev server（HTTP + WebSocket Upgrade 透传）
pub async fn forward(req: Request<Incoming>, port: u16) -> Result<Response<Body>, BoxError> {
    let path_and_query = req
        .uri()
        .path_and_query()
        .map(|p| p.as_str().to_string())
        .unwrap_or_else(|| "/".to_string());
    let method = req.method().clone();

    let mut req = req;
    let client_upgrade = hyper::upgrade::on(&mut req);

    let mut builder = Request::builder().method(method).uri(path_and_query);
    {
        let headers = builder
            .headers_mut()
            .ok_or_else(|| -> BoxError { "无法构建上游请求头".into() })?;
        for (name, value) in req.headers() {
            if skip_request_header(name.as_str()) {
                continue;
            }
            headers.append(name.clone(), value.clone());
        }
        headers.insert(HOST, HeaderValue::from_str(&format!("localhost:{}", port))?);
        // 部分框架（Next 等）会校验 WebSocket Origin 与 Host 一致，统一改写为本机上游地址
        if headers.contains_key(ORIGIN) {
            headers.insert(
                ORIGIN,
                HeaderValue::from_str(&format!("http://localhost:{}", port))?,
            );
        }
    }

    let upstream_req = builder.body(
        req.into_body()
            .map_err(|e| -> BoxError { Box::new(e) })
            .boxed(),
    )?;

    // ponytail: 每请求新建一条上游连接，本地 dev 场景足够；需要吞吐时再加连接池
    // 用 localhost 解析：Vite 等框架默认可能只监听 ::1
    let stream = TcpStream::connect(("localhost", port)).await?;
    let (mut sender, conn) = hyper::client::conn::http1::handshake(TokioIo::new(stream)).await?;
    tokio::spawn(async move {
        let _ = conn.with_upgrades().await;
    });

    let mut resp = sender.send_request(upstream_req).await?;
    let status = resp.status();
    let switching = status == StatusCode::SWITCHING_PROTOCOLS;

    if switching {
        let upstream_upgrade = hyper::upgrade::on(&mut resp);
        tokio::spawn(async move {
            if let (Ok(client), Ok(upstream)) = (client_upgrade.await, upstream_upgrade.await) {
                let mut client = TokioIo::new(client);
                let mut upstream = TokioIo::new(upstream);
                let _ = tokio::io::copy_bidirectional(&mut client, &mut upstream).await;
            }
        });
    }

    let mut builder = Response::builder().status(status);
    {
        let headers = builder
            .headers_mut()
            .ok_or_else(|| -> BoxError { "无法构建下游响应头".into() })?;
        for (name, value) in resp.headers() {
            // hyper 自行处理分帧，透传 transfer-encoding 反而会破坏响应
            if name == TRANSFER_ENCODING {
                continue;
            }
            if !switching && is_hop_by_hop(name.as_str()) {
                continue;
            }
            headers.append(name.clone(), value.clone());
        }
    }

    Ok(builder.body(
        resp.into_body()
            .map_err(|e| -> BoxError { Box::new(e) })
            .boxed(),
    )?)
}

fn skip_request_header(name: &str) -> bool {
    matches!(
        name,
        "host"
            | "transfer-encoding"
            | "proxy-connection"
            | "proxy-authorization"
            | "proxy-authenticate"
            | "te"
            | "trailer"
            | "keep-alive"
    )
}

fn is_hop_by_hop(name: &str) -> bool {
    matches!(
        name,
        "connection"
            | "keep-alive"
            | "proxy-authenticate"
            | "proxy-authorization"
            | "te"
            | "trailer"
            | "transfer-encoding"
            | "upgrade"
    )
}
