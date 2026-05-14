//! 多机分片压测的运行参数（`perp_bench`、`rooter_deposit` 等共用）。
//!
//! **全部通过进程环境变量 `std::env` 读取**。启动时由 [`ShardRunConfig::load`] 先加载 dotenv 文件进环境，再解析：
//! - 若设置 **`ENV_FILE`**：从该路径加载（文件须存在）；
//! - 否则：若当前工作目录存在 **`.env`**，则加载（不存在则忽略）。
//!
//! 已在 shell 里 `export` 的变量 **不会被** dotenv 覆盖（与 `dotenvy` 默认行为一致）。
//!
//! | 键 / 环境变量 | 含义 | 默认 |
//! |----------------|------|------|
//! | `WS_URL` | 链 WebSocket RPC | `ws://136.110.109.17:9937` |
//! | `FIRST_ADDR_INDEX` | 第一个 `DerivationPath::eth(0, addr_idx)` 的 `addr_idx` | `1001` |
//! | `ACCOUNT_COUNT` | 本进程账户数 | `20` |
//! | `RATE` | 发压频率（笔/秒），`perp_bench` 使用 | `100` |
//! | `RUN_ID` | 可选，日志用 | 无 |

use std::env;
use std::path::PathBuf;

const DEFAULT_WS: &str = "ws://136.110.109.17:9937";

/// 将 `ENV_FILE` 或默认 `.env` 载入进程环境（不覆盖已有变量）。
fn load_dotenv_into_env() -> anyhow::Result<()> {
    match env::var("ENV_FILE") {
        Ok(raw) => {
            let path = PathBuf::from(raw.trim());
            if !path.is_file() {
                anyhow::bail!("ENV_FILE 不是可读文件: {}", path.display());
            }
            dotenvy::from_path(&path)
                .map_err(|e| anyhow::anyhow!("读取 ENV_FILE {}: {}", path.display(), e))?;
        }
        Err(_) => {
            let _ = dotenvy::dotenv();
        }
    }
    Ok(())
}

fn pick_string(key: &str, default: &str) -> String {
    env::var(key)
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| default.to_string())
}

fn pick_u32(key: &str, default: u32) -> anyhow::Result<u32> {
    let s = pick_string(key, "");
    if s.is_empty() {
        return Ok(default);
    }
    s.parse::<u32>()
        .map_err(|e| anyhow::anyhow!("{key}={s:?} 不是合法 u32: {e}"))
}

#[derive(Debug, Clone)]
pub struct ShardRunConfig {
    pub ws_url: String,
    pub first_addr_index: u32,
    pub account_count: u32,
    pub rate: u32,
    pub run_id: Option<String>,
}

impl ShardRunConfig {
    /// 先 `dotenvy` 加载 `.env` / `ENV_FILE`，再从 **`std::env`** 读取各键。
    pub fn load() -> anyhow::Result<Self> {
        load_dotenv_into_env()?;

        let ws_url = pick_string("WS_URL", DEFAULT_WS);
        if ws_url.is_empty() {
            anyhow::bail!("WS_URL 为空");
        }

        let first_addr_index = pick_u32("FIRST_ADDR_INDEX", 1001)?;
        let account_count = pick_u32("ACCOUNT_COUNT", 20)?;
        if account_count == 0 {
            anyhow::bail!("ACCOUNT_COUNT 必须 > 0");
        }
        if first_addr_index < 1 {
            anyhow::bail!("FIRST_ADDR_INDEX 必须 >= 1");
        }

        let rate = pick_u32("RATE", 100)?;
        if rate == 0 {
            anyhow::bail!("RATE 必须 > 0");
        }

        let run_id_raw = pick_string("RUN_ID", "");
        let run_id = if run_id_raw.is_empty() {
            None
        } else {
            Some(run_id_raw)
        };

        Ok(Self {
            ws_url,
            first_addr_index,
            account_count,
            rate,
            run_id,
        })
    }

    /// 与 [`load`](Self::load) 相同，保留旧名称。
    #[inline]
    pub fn from_env() -> anyhow::Result<Self> {
        Self::load()
    }

    pub fn summary(&self) -> String {
        format!(
            "run_id={:?} ws={} addr_idx=[{}..{}] (count={}) rate={}",
            self.run_id,
            self.ws_url,
            self.first_addr_index,
            self.first_addr_index.saturating_add(self.account_count.saturating_sub(1)),
            self.account_count,
            self.rate,
        )
    }
}
