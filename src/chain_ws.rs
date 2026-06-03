//! 由 `rooter_deposit`、`perp_bench` 等在启动时写入，供本 crate 内 `get_api` / `get_rpc` 使用（避免写死 WS 地址）。

use std::sync::OnceLock;
use std::time::Duration;
use subxt::backend::rpc::RpcClient;
use tokio::sync::Mutex;

static WS_URL: OnceLock<String> = OnceLock::new();
static SHARED_RPC: Mutex<Option<RpcClient>> = Mutex::const_new(None);

/// 必须在首次调用 `rpc_client` 之前调用一次。
pub fn init(url: String) -> Result<(), &'static str> {
    log::info!("chain WS: {url}");
    WS_URL.set(url).map_err(|_| "chain WS URL 已初始化，请勿重复 init")
}

pub fn ws_url() -> &'static str {
    WS_URL
        .get()
        .expect("chain WS 未初始化：请先调用 subtx_test::chain_ws::init(url)")
        .as_str()
}

fn connect_timeout_secs() -> u64 {
    std::env::var("WS_CONNECT_TIMEOUT_SEC")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(60)
}

fn connect_retries() -> u32 {
    std::env::var("WS_CONNECT_RETRIES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(5)
}

async fn connect_fresh() -> Result<RpcClient, String> {
    let url = ws_url();
    let retries = connect_retries();
    let outer = Duration::from_secs(connect_timeout_secs());
    let mut last_err = String::new();
    for attempt in 1..=retries {
        match tokio::time::timeout(outer, RpcClient::from_insecure_url(url)).await {
                    Ok(Ok(client)) => {
                        log::debug!("RPC 已连接 {url}");
                        return Ok(client);
                    }
            Ok(Err(e)) => {
                last_err = format!("{e:?}");
                log::warn!("RPC 连接失败 (尝试 {attempt}/{retries}): {last_err}");
            }
            Err(_) => {
                last_err = format!("外层等待超过 {}s", outer.as_secs());
                log::warn!("RPC 连接超时 (尝试 {attempt}/{retries}): {url}");
            }
        }
        if attempt < retries {
            tokio::time::sleep(Duration::from_secs(3)).await;
        }
    }
    Err(format!(
        "无法连接 {url}（已重试 {retries} 次）。请检查节点是否运行、WS_URL 端口是否正确（常见 9923/9944）、本机到 {url} 网络是否可达。最后错误: {last_err}"
    ))
}

/// 带重试的 WebSocket RPC 连接（默认最多 5 次、单次外层等待 60s）。
pub async fn rpc_client() -> Result<RpcClient, String> {
    let guard = SHARED_RPC.lock().await;
    if let Some(c) = guard.as_ref() {
        return Ok(c.clone());
    }
    drop(guard);
    reconnect_rpc().await
}

/// 丢弃缓存连接（RPC 后台任务断开时由调用方触发）。
pub async fn reset_rpc() {
    let mut guard = SHARED_RPC.lock().await;
    *guard = None;
    log::debug!("RPC 缓存已清除，下次调用将重连");
}

/// 强制新建连接并写入缓存。
pub async fn reconnect_rpc() -> Result<RpcClient, String> {
    let client = connect_fresh().await?;
    let mut guard = SHARED_RPC.lock().await;
    *guard = Some(client.clone());
    Ok(client)
}

pub fn is_rpc_disconnect(err: &impl std::fmt::Display) -> bool {
    let s = err.to_string().to_lowercase();
    s.contains("connection closed")
        || s.contains("restart required")
        || s.contains("background task closed")
        || s.contains("closed connection")
        || s.contains("not connected")
        || s.contains("ws connection")
}
