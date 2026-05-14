//! 由 `rooter_deposit`、`perp_bench` 等在启动时写入，供本 crate 内 `get_api` / `get_rpc` 使用（避免写死 WS 地址）。

use std::sync::OnceLock;

static WS_URL: OnceLock<String> = OnceLock::new();

/// 必须在首次调用 `get_api` / `get_rpc` 之前调用一次。
pub fn init(url: String) -> Result<(), &'static str> {
    WS_URL.set(url).map_err(|_| "chain WS URL 已初始化，请勿重复 init")
}

pub fn ws_url() -> &'static str {
    WS_URL
        .get()
        .expect("chain WS 未初始化：请先调用 subtx_test::chain_ws::init(url)")
        .as_str()
}
