//! # 永续压测 — **栅栏 + 池化批量提交**（`perp_bench_fence`）
//!
//! 与 `perp_bench` **共用**环境变量与前置流程（`ShardRunConfig::load`、`BENCH_*`、准备阶段等），**不修改** `perp_bench.rs`。
//!
//! ## 与 `perp_bench` 的差异
//!
//! 1. **编签阶段**：每账户预先构造 `n = RATE * BENCH_DURATION_SEC` 笔 **place / cancel 交替** 的已签名 payload（nonce 仍为时间戳底 + 递增，与 `perp_bench` 一致）。
//! 2. **栅栏**：所有账户编签任务 **全部完成后** 才进入发送阶段（`JoinSet` 自然形成栅栏）。
//! 3. **发送阶段**：`FENCE_POOL_SENDERS` 个池 worker 从 channel 收 **`RATE` 笔为一块`** 的 `Vec<Bytes>`，凑满 `pool_rate` 笔后调用 **`author_submit_extrinsics`**（格式与 `testnet_test` 一致：`Compact(笔数)` 前缀 + 裸编码拼接）。**批间节流以铺满 `BENCH_DURATION_SEC`**：与 `testnet_test` 池化路径一致，每个 worker 在**每一批** RPC 前若距上一批不足 **1 秒**则 `sleep`，使全网约 `ACCOUNT_COUNT×RATE` 笔/秒、总墙钟约 **`BENCH_DURATION_SEC`**（`FENCE_BURST_SUBMIT=1` 可关闭节流、恢复瞬时灌满）。
//! 4. **链上核对**：发送前、发送结束并等待 **`FENCE_POST_CHAIN_SCAN_SEC`** 后各扫一遍 storage，按 `testnet_test` 同款口径统计 **`next_order_id−1` 累计增量**（反映已分配订单号的 **place 上链** 情况；与 `submit_ok` 的 RPC 逐条成功数不同）。
//!
//! ## 内存与风险
//!
//! 内存约 **账户数 × n × 单笔编码大小**；大账户数、长 `BENCH_DURATION_SEC` 时请自行评估机器 RAM。编签并发受 **`CHAIN_SCAN_CONCURRENCY`** 限制，以降低瞬时 CPU/内存尖峰。
//!
//! ## 环境变量（`ShardRunConfig::load()` 会加载 `.env`）
//!
//! | 变量 | 含义 | 默认 |
//! |------|------|------|
//! | `BENCH_MARKET_ID` | **发单/撤单/平仓** 使用的永续 `market_id` | `3` |
//! | `BENCH_ORDER_SIZE` | `place_order` 的 `size` | `1000000000000000` |
//! | `BENCH_MATCHED_PERCENT` | 与 `testnet_test` 的 `MATCHED_PERCENT` 同义 | `0` |
//! | `BENCH_DURATION_SEC` | 每账户预编笔数 = `RATE ×` 本项；发送节流墙钟约本项秒数 | `60` |
//! | `BENCH_PREP_CONCURRENCY` / `BENCH_SKIP_PREP` | 准备阶段 | 见 `perp_bench` |
//! | `FENCE_POOL_SENDERS` | 池 worker 数量（与 `testnet_test` 的 `pool_sender_num` 同角色） | `20` |
//! | `FENCE_THROTTLE_SEC` | 每批 RPC **之后**额外睡眠秒数（默认 `0`） | `0` |
//! | `FENCE_BURST_SUBMIT` | 设为 `1`/`true` 时**关闭**批间 1s 节流（旧行为：尽快发完） | 关 |
//! | `FENCE_POST_CHAIN_SCAN_SEC` | 发送结束后等待多少秒再扫链核对；`0` 跳过链上汇总 | `5` |
//!
//! ```bash
//! RUST_LOG=info cargo run --release --bin perp_bench_fence
//! ```

#![allow(missing_docs)]

use std::collections::HashMap;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use subxt::backend::rpc::RpcClient;
use subxt::config::substrate::SubstrateExtrinsicParamsBuilder;
use subxt::config::{substrate, SubstrateExtrinsicParams, Config};
use subxt::ext::codec::{Compact, Encode};
use subxt::ext::subxt_core::utils::AccountId20;
use subxt::ext::subxt_rpcs::LegacyRpcMethods;
use subxt::utils::H160;
use subxt::OnlineClient;
use subxt_signer::eth::Signature;
use subxt_signer::eth::{DerivationPath, Keypair};
use subxt_signer::{bip39, DEV_PHRASE};
use tokio::sync::{mpsc::UnboundedReceiver, mpsc::UnboundedSender, OnceCell, Semaphore};
use tokio::task::JoinSet;

use log::{info, warn};
use subtx_test::chain_ws;
use subtx_test::shard_run_config::{env_truthy, env_u16, env_u64, ShardRunConfig};

#[subxt::subxt(
    runtime_metadata_path = "./deepx-node-metadata.scale",
    derive_for_all_types = "Eq, PartialEq, Clone, Debug"
)]
pub mod node_runtime {}

use node_runtime::perp_market::calls::types::cancel_order::CancelReason;
use node_runtime::perp_market::calls::types::place_order::OrderType;
use node_runtime::runtime_types::pallet_primitives::types::PostOnlyParam;

#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub enum EthRuntimeConfig {}

impl Config for EthRuntimeConfig {
    type AccountId = AccountId20;
    type Address = AccountId20;
    type Signature = Signature;
    type Hasher = substrate::BlakeTwo256;
    type Header = substrate::SubstrateHeader<u32, substrate::BlakeTwo256>;
    type ExtrinsicParams = SubstrateExtrinsicParams<Self>;
    type AssetId = u32;
}

static GLOBAL_API: OnceCell<OnlineClient<EthRuntimeConfig>> = OnceCell::const_new();
static GLOBAL_RPC: OnceCell<RpcClient> = OnceCell::const_new();

async fn get_api() -> anyhow::Result<OnlineClient<EthRuntimeConfig>> {
    let api = GLOBAL_API
        .get_or_try_init(|| async {
            OnlineClient::<EthRuntimeConfig>::from_insecure_url(chain_ws::ws_url()).await
        })
        .await?;
    Ok(api.clone())
}

async fn get_rpc() -> anyhow::Result<LegacyRpcMethods<EthRuntimeConfig>> {
    let rpc_client = GLOBAL_RPC
        .get_or_try_init(|| async { RpcClient::from_insecure_url(chain_ws::ws_url()).await })
        .await?;
    Ok(LegacyRpcMethods::new(rpc_client.clone()))
}

struct BenchExtra {
    duration_sec: u64,
    scan_concurrency: usize,
    prep_concurrency: usize,
    start_at_unix_ms: Option<u64>,
    skip_prep: bool,
    market_id: u16,
    order_size: u128,
    matched_percent: u32,
    pool_senders: u32,
    throttle_sec: u64,
    /// 关闭批间 1s 节流，与旧版栅栏「瞬时灌满」一致。
    burst_submit: bool,
    /// 发送结束后 sleep 再扫链；`0` 不做链上汇总。
    post_chain_scan_sec: u64,
}

impl BenchExtra {
    fn from_env() -> anyhow::Result<Self> {
        let matched_percent: u32 = env_u64("BENCH_MATCHED_PERCENT", 0).min(100) as u32;
        Ok(Self {
            duration_sec: env_u64("BENCH_DURATION_SEC", 60).max(1),
            scan_concurrency: env_u64("CHAIN_SCAN_CONCURRENCY", 64).clamp(1, 512) as usize,
            prep_concurrency: env_u64("BENCH_PREP_CONCURRENCY", 32).clamp(1, 256) as usize,
            start_at_unix_ms: std::env::var("BENCH_START_AT_UNIX_MS")
                .ok()
                .and_then(|s| s.parse().ok()),
            skip_prep: env_truthy("BENCH_SKIP_PREP"),
            market_id: env_u16("BENCH_MARKET_ID", 3)?,
            order_size: env_u64("BENCH_ORDER_SIZE", 1_000_000_000_000_000) as u128,
            matched_percent,
            pool_senders: env_u64("FENCE_POOL_SENDERS", 20).clamp(1, 256) as u32,
            throttle_sec: env_u64("FENCE_THROTTLE_SEC", 0).min(3600),
            burst_submit: env_truthy("FENCE_BURST_SUBMIT"),
            post_chain_scan_sec: env_u64("FENCE_POST_CHAIN_SCAN_SEC", 5).min(600),
        })
    }
}

#[derive(Clone)]
struct Account {
    name: String,
    kp: Keypair,
    subaccount: H160,
    next_order_id: u32,
    is_long: bool,
    limit_price: u128,
}

static GLOBAL_SUBMIT_OK: AtomicU64 = AtomicU64::new(0);
static GLOBAL_SUBMIT_ERR: AtomicU64 = AtomicU64::new(0);

/// 汇总 `author_submit_extrinsics` 批内/整批失败信息（限流打印，避免刷屏）。
static SUBMIT_ERR_AGG: OnceLock<Mutex<SubmitErrAgg>> = OnceLock::new();

#[derive(Default)]
struct SubmitErrAgg {
    sample_lines: usize,
    by_msg: HashMap<String, u64>,
    pool_limit_hits: u64,
    batch_len_mismatch: u64,
}

fn submit_err_text(err: &impl std::fmt::Debug) -> String {
    format!("{err:?}")
}

/// 节点 tx-pool 在 ready 池满（`--pool-limit`）时会返回 `ImmediatelyDropped` 或整批 `Pool(...)`。
fn is_pool_limit_msg(msg: &str) -> bool {
    let lower = msg.to_ascii_lowercase();
    lower.contains("immediatelydropped")
        || lower.contains("couldn't enter the pool because of the limit")
        || lower.contains("pool locked")
        || (lower.contains("pool") && lower.contains("limit"))
}

impl SubmitErrAgg {
    const MAX_SAMPLES: usize = 32;
    const MAX_MSG_LEN: usize = 320;

    fn reset() {
        if let Some(m) = SUBMIT_ERR_AGG.get() {
            *m.lock().expect("submit err agg lock") = SubmitErrAgg::default();
        }
    }

    fn truncate_key(raw: &str) -> String {
        if raw.len() > Self::MAX_MSG_LEN {
            format!("{}…", &raw[..Self::MAX_MSG_LEN])
        } else {
            raw.to_string()
        }
    }

    fn record_str(&mut self, ctx: &str, raw: &str) {
        let key = Self::truncate_key(raw);
        let pool_hit = is_pool_limit_msg(&key);
        if pool_hit {
            self.pool_limit_hits += 1;
        }
        *self.by_msg.entry(key.clone()).or_default() += 1;
        if pool_hit {
            warn!("发压 submit 失败 [pool limit 相关] [{ctx}]: {key}");
        } else if self.sample_lines < Self::MAX_SAMPLES {
            warn!("发压 submit 失败 [{ctx}]: {key}");
            self.sample_lines += 1;
        }
    }

    fn record(&mut self, ctx: &str, err: &impl std::fmt::Debug) {
        self.record_str(ctx, &submit_err_text(err));
    }

    fn record_batch_len_mismatch(&mut self, expected: u32, got: usize) {
        self.batch_len_mismatch += 1;
        warn!(
            "author_submit_extrinsics 返回条数与提交笔数不一致: 提交={expected} 返回={got}（未计入的笔既非 ok 也非 err，请核对节点 RPC）"
        );
    }

    fn log_summary(&self, rpc_ok: u64, rpc_err: u64) {
        if self.batch_len_mismatch > 0 {
            warn!(
                "发压期间 author_submit_extrinsics 返回条数不一致共 {} 次",
                self.batch_len_mismatch
            );
        }
        if self.pool_limit_hits > 0 {
            warn!(
                "检测到 pool limit 相关 RPC 拒绝共 {} 次（关键词 ImmediatelyDropped / pool+limit）；开发问的「有没有报 pool limit」→ **有**",
                self.pool_limit_hits
            );
        } else if rpc_err == 0 && rpc_ok > 0 {
            info!(
                "未检测到 pool limit 相关 RPC 错误（ImmediatelyDropped 等）；开发问的「有没有报 pool limit」→ **本次 run 的 submit 路径上没有**"
            );
        }
        if self.by_msg.is_empty() {
            if rpc_err == 0 && rpc_ok > 0 {
                info!(
                    "发压 RPC 批内无 Err 返回（submit_err=0）。说明：Ok 仅表示节点接受交易进池，不等于链上 place 执行成功；若订单号增量偏低请看「异常账户诊断」"
                );
            }
            return;
        }
        let mut rows: Vec<_> = self.by_msg.iter().collect();
        rows.sort_by(|a, b| b.1.cmp(a.1));
        warn!(
            "发压 submit 错误汇总（RPC submit_err={}，下列为去重后的错误类型，pool limit 类见上方；至多 {} 条非 pool 样本见 warn）:",
            rpc_err,
            self.sample_lines
        );
        for (msg, cnt) in rows.iter().take(16) {
            let tag = if is_pool_limit_msg(msg) {
                "[pool limit] "
            } else {
                ""
            };
            warn!("  ×{cnt} {tag}{msg}");
        }
        if rows.len() > 16 {
            warn!("  … 另有 {} 种错误未列出", rows.len() - 16);
        }
    }
}

fn record_submit_err(ctx: &str, err: &impl std::fmt::Debug) {
    let Some(m) = SUBMIT_ERR_AGG.get() else {
        warn!("发压 submit 失败 [{ctx}]: {}", submit_err_text(err));
        return;
    };
    m.lock().expect("submit err agg lock").record(ctx, err);
}

fn log_submit_err_summary(rpc_ok: u64, rpc_err: u64) {
    if let Some(m) = SUBMIT_ERR_AGG.get() {
        m.lock().expect("submit err agg lock").log_summary(rpc_ok, rpc_err);
    }
}

/// 与 `testnet_test` 收尾一致：`subaccount_info.next_order_id - 1` 为累计订单号末端；另带挂单与仓位手数便于对照。
#[derive(Clone, Copy, Default, Debug)]
struct ChainLineStat {
    cumulative_order_excl: u32,
    pending_on_market: u32,
    matched_lots: u128,
}

async fn chain_stats_one(
    subaccount: H160,
    market_id: u16,
    order_size: u128,
) -> anyhow::Result<ChainLineStat> {
    let api = get_api().await?;
    let info_q = node_runtime::storage()
        .subaccount()
        .subaccount_info(subaccount.clone());
    let info = api.storage().at_latest().await?.fetch(&info_q).await?;
    let cumulative_order_excl = info
        .as_ref()
        .map(|i| i.next_order_id.saturating_sub(1))
        .unwrap_or(0);

    let pos_q = node_runtime::storage()
        .perp_market()
        .user_perp_positions(subaccount.clone());
    let positions = api
        .storage()
        .at_latest()
        .await?
        .fetch(&pos_q)
        .await?
        .unwrap_or_default();
    let sz = order_size.max(1);
    let matched_lots = positions
        .iter()
        .find(|p| p.market_id == market_id)
        .map(|p| {
            let amt = p.base_asset_amount;
            amt / sz
        })
        .unwrap_or(0);

    let ord_q = node_runtime::storage()
        .perp_market()
        .active_perp_orders_for(subaccount);
    let ord_wrap = api.storage().at_latest().await?.fetch(&ord_q).await?;
    let pending_on_market = ord_wrap
        .as_ref()
        .map(|orders| {
            orders
                .iter()
                .filter(|(mid, _)| *mid == market_id)
                .map(|(_, v)| v.len())
                .sum::<usize>() as u32
        })
        .unwrap_or(0);

    Ok(ChainLineStat {
        cumulative_order_excl,
        pending_on_market,
        matched_lots,
    })
}

async fn fetch_chain_stats_parallel(
    keys: &[(String, H160)],
    market_id: u16,
    order_size: u128,
    concurrency: usize,
) -> anyhow::Result<Vec<ChainLineStat>> {
    let n = keys.len();
    let mut out: Vec<Option<ChainLineStat>> = vec![None; n];
    for chunk_start in (0..n).step_by(concurrency) {
        let end = (chunk_start + concurrency).min(n);
        let mut set = JoinSet::new();
        for i in chunk_start..end {
            let sub = keys[i].1;
            set.spawn(async move {
                let st = chain_stats_one(sub, market_id, order_size).await?;
                Ok::<_, anyhow::Error>((i, st))
            });
        }
        while let Some(j) = set.join_next().await {
            let (idx, st) = j??;
            out[idx] = Some(st);
        }
    }
    out.into_iter()
        .map(|x| x.ok_or_else(|| anyhow::anyhow!("chain stat join internal")))
        .collect()
}

fn log_chain_reconcile(
    keys: &[(String, H160)],
    pre: &[ChainLineStat],
    post: &[ChainLineStat],
    places_per_account: u32,
    market_id: u16,
    sleep_sec: u64,
    rpc_ok: u64,
    rpc_err: u64,
) {
    let n_acc = keys.len();
    if n_acc == 0 || pre.len() != n_acc || post.len() != n_acc {
        warn!("链上核对: 内部长度不一致，跳过汇总");
        return;
    }

    let expect_order_delta: u64 = (places_per_account as u64).saturating_mul(n_acc as u64);
    let mut sum_delta_ord: u64 = 0;
    let mut sum_pre_pend: u64 = 0;
    let mut sum_post_pend: u64 = 0;
    let mut sum_pre_matched: u128 = 0;
    let mut sum_post_matched: u128 = 0;
    let mut weak_accounts: Vec<(String, u32)> = Vec::new();

    for i in 0..n_acc {
        let d = post[i]
            .cumulative_order_excl
            .saturating_sub(pre[i].cumulative_order_excl);
        sum_delta_ord = sum_delta_ord.saturating_add(d as u64);
        sum_pre_pend = sum_pre_pend.saturating_add(pre[i].pending_on_market as u64);
        sum_post_pend = sum_post_pend.saturating_add(post[i].pending_on_market as u64);
        sum_pre_matched = sum_pre_matched.saturating_add(pre[i].matched_lots);
        sum_post_matched = sum_post_matched.saturating_add(post[i].matched_lots);
        if d < places_per_account {
            weak_accounts.push((keys[i].0.clone(), d));
        }
    }

    let dm = sum_post_matched.saturating_sub(sum_pre_matched);
    info!(
        "链上核对(发送结束后已等待 {}s): 全户累计订单号末端(next_order_id−1) 增量合计={}；若每笔 place 均上链则理论约 {} (= {} 户 × 每账户 place 笔数 {})",
        sleep_sec, sum_delta_ord, expect_order_delta, n_acc, places_per_account
    );
    info!(
        "链上 market={}: 挂单数合计 pre={} → post={}；matched_lots 全户合计 pre={} post={}（Δ={}，撮合/仓位变化可参考）",
        market_id, sum_pre_pend, sum_post_pend, sum_pre_matched, sum_post_matched, dm
    );
    info!(
        "RPC 批量返回（含 place+cancel 逐条）: submit_ok={} submit_err={} — 与上列「订单号增量」口径不同，不能直接等同「上链 place 笔数」",
        rpc_ok, rpc_err
    );

    let threshold = expect_order_delta.saturating_mul(9) / 10;
    if expect_order_delta > 0 && sum_delta_ord < threshold {
        warn!(
            "链上订单号增量 {} 低于理论 place 上链量 {} 的约 90%（{}），可能存在丢交易、未打包或 RPC 成功但执行失败等情况",
            sum_delta_ord, expect_order_delta, threshold
        );
    }

    const CAP: usize = 8;
    if !weak_accounts.is_empty() {
        weak_accounts.sort_by(|a, b| a.1.cmp(&b.1));
        let show = weak_accounts.len().min(CAP);
        warn!(
            "订单号增量低于每账户 place 数({}) 的账户（至多列 {} 个，按增量升序）: {:?}",
            places_per_account,
            show,
            &weak_accounts[..show]
        );
        if weak_accounts.len() > CAP {
            warn!("… 另有 {} 个账户未列出", weak_accounts.len() - CAP);
        }
    }
}

/// 对订单号增量偏低的账户打印链上状态，便于区分「未发出去」与「RPC Ok 但执行失败」。
async fn diagnose_weak_accounts(
    keys: &[(String, H160)],
    pre: &[ChainLineStat],
    post: &[ChainLineStat],
    places_per_account: u32,
) -> anyhow::Result<()> {
    let n_acc = keys.len();
    if n_acc == 0 || pre.len() != n_acc || post.len() != n_acc {
        return Ok(());
    }
    let mut weak: Vec<usize> = (0..n_acc)
        .filter(|&i| {
            post[i]
                .cumulative_order_excl
                .saturating_sub(pre[i].cumulative_order_excl)
                < places_per_account
        })
        .collect();
    if weak.is_empty() {
        return Ok(());
    }
    weak.sort_by_key(|&i| {
        post[i]
            .cumulative_order_excl
            .saturating_sub(pre[i].cumulative_order_excl)
    });

    const CAP: usize = 12;
    warn!(
        "异常账户诊断（订单号增量 < 每账户 place 数 {}）：共 {} 户，展开前 {} 户",
        places_per_account,
        weak.len(),
        weak.len().min(CAP)
    );

    let api = get_api().await?;
    for &i in weak.iter().take(CAP) {
        let (name, sub) = &keys[i];
        let delta = post[i]
            .cumulative_order_excl
            .saturating_sub(pre[i].cumulative_order_excl);
        let info_q = node_runtime::storage().subaccount().subaccount_info(sub.clone());
        let info = api.storage().at_latest().await?.fetch(&info_q).await?;
        let info_hint = match &info {
            Some(inf) => format!(
                "存在 next_order_id={}",
                inf.next_order_id
            ),
            None => "缺失（子账户未初始化？）".to_string(),
        };
        warn!(
            "  {name} sub={sub:?} Δorder_id={delta} cumulative {c0}→{c1} pending {p0}→{p1} matched_lots {m0}→{m1} | {info_hint}",
            c0 = pre[i].cumulative_order_excl,
            c1 = post[i].cumulative_order_excl,
            p0 = pre[i].pending_on_market,
            p1 = post[i].pending_on_market,
            m0 = pre[i].matched_lots,
            m1 = post[i].matched_lots,
        );
        if delta == 0 {
            warn!(
                "    → 该户链上无新订单号：若上方 submit_err=0，多为交易进池但未成功执行（nonce/签名/配额/Invalid signing address 等），节点批量 RPC 通常不回传执行错误"
            );
        }
    }
    if weak.len() > CAP {
        warn!("  … 另有 {} 户未展开", weak.len() - CAP);
    }
    Ok(())
}

fn derive_accounts(first: u32, count: u32) -> anyhow::Result<Vec<(String, Keypair)>> {
    let mut v = Vec::with_capacity(count as usize);
    for i in 0..count {
        let addr_idx = first + i;
        let kp = Keypair::from_phrase(
            &bip39::Mnemonic::from_str(DEV_PHRASE)?,
            None,
            DerivationPath::eth(0, addr_idx),
        )?;
        v.push((format!("test_user_{addr_idx}"), kp));
    }
    Ok(v)
}

async fn resolve_accounts_parallel(
    rows: Vec<(String, Keypair)>,
    concurrency: usize,
) -> anyhow::Result<Vec<Account>> {
    let n = rows.len();
    let mut out: Vec<Option<Account>> = vec![None; n];
    for chunk_start in (0..n).step_by(concurrency) {
        let end = (chunk_start + concurrency).min(n);
        let mut set = JoinSet::new();
        for i in chunk_start..end {
            let (name, kp) = rows[i].clone();
            set.spawn(async move {
                let api = get_api().await?;
                let q = node_runtime::storage()
                    .subaccount()
                    .user_stats_for(kp.public_key().to_account_id().0.into());
                let st = api.storage().at_latest().await?.fetch(&q).await?;
                let Some(st) = st else {
                    anyhow::bail!("{name}: 无 user_stats（未初始化子账户？）");
                };
                let Some(sub) = st.subaccounts.first().cloned() else {
                    anyhow::bail!("{name}: user_stats 无子账户");
                };
                let info_q = node_runtime::storage().subaccount().subaccount_info(sub.clone());
                let info = api
                    .storage()
                    .at_latest()
                    .await?
                    .fetch(&info_q)
                    .await?
                    .ok_or_else(|| anyhow::anyhow!("{name}: subaccount_info 缺失"))?;
                Ok::<_, anyhow::Error>((
                    i,
                    Account {
                        name,
                        kp,
                        subaccount: sub,
                        next_order_id: info.next_order_id,
                        is_long: false,
                        limit_price: 0,
                    },
                ))
            });
        }
        while let Some(j) = set.join_next().await {
            let (idx, acc) = j??;
            out[idx] = Some(acc);
        }
        info!("resolve 子账户进度 {}/{}", end, n);
    }
    out.into_iter()
        .map(|x| x.ok_or_else(|| anyhow::anyhow!("internal")))
        .collect()
}

/// 链上 perp 市场报价上下文（与 `PerpOraclePriceGuard` 偏差带一致）。
struct MarketQuoteCtx {
    mark_price: u128,
    tick_size: u128,
    lower: u128,
    upper: u128,
}

async fn load_market_quote_ctx(market_id: u16) -> anyhow::Result<MarketQuoteCtx> {
    let api = get_api().await?;
    let q = node_runtime::storage().perp_market().perp_markets(market_id);
    let m = api
        .storage()
        .at_latest()
        .await?
        .fetch(&q)
        .await?
        .ok_or_else(|| anyhow::anyhow!("market {market_id} 不存在"))?;
    let mark_price = m.mark_price as u128;
    let tick_size = m.order_spec.tick_size as u128;
    anyhow::ensure!(tick_size > 0, "market {market_id} tick_size 为 0");
    let dev = m.max_deviation_bps as u128;
    let lower = mark_price.saturating_mul(10_000u128.saturating_sub(dev)) / 10_000;
    let upper = mark_price.saturating_mul(10_000u128.saturating_add(dev)) / 10_000;
    Ok(MarketQuoteCtx {
        mark_price,
        tick_size,
        lower,
        upper,
    })
}

fn prune_price_to_tick(price: u128, tick: u128) -> u128 {
    if tick == 0 {
        return price;
    }
    price.saturating_sub(price % tick)
}

/// 在 oracle 偏差带内、按链上 tick 对齐的限价（多空各偏移若干 tick，避免低价币大偏移越界）。
fn limit_price_for_account(ctx: &MarketQuoteCtx, account_index: usize, is_long: bool) -> u128 {
    let n = (account_index as u128).saturating_add(1);
    let anchor = prune_price_to_tick(ctx.mark_price, ctx.tick_size);
    let off = ctx.tick_size.saturating_mul(n);
    let raw = if is_long {
        anchor.saturating_sub(off)
    } else {
        anchor.saturating_add(off)
    };
    let clamped = raw.clamp(ctx.lower, ctx.upper);
    prune_price_to_tick(clamped, ctx.tick_size)
}

async fn assign_quotes(mut accounts: Vec<Account>, market_id: u16) -> anyhow::Result<Vec<Account>> {
    let ctx = load_market_quote_ctx(market_id).await?;
    for (i, a) in accounts.iter_mut().enumerate() {
        a.is_long = i % 2 == 0;
        a.limit_price = limit_price_for_account(&ctx, i, a.is_long);
    }
    info!(
        "限价分配 market={}: mark={} tick={} band=[{}, {}]（按链上 max_deviation_bps）",
        market_id, ctx.mark_price, ctx.tick_size, ctx.lower, ctx.upper
    );
    for a in accounts.iter().take(8) {
        let side = if a.is_long { "long" } else { "short" };
        info!("  [{}] {side} limit_price={}", a.name, a.limit_price);
    }
    if accounts.len() > 8 {
        info!("  … 另有 {} 户未列出", accounts.len() - 8);
    }
    Ok(accounts)
}

fn target_price(
    matched_percent: u32,
    _is_long: bool,
    pre_price: u128,
    match_price: u128,
    cancel_id: u32,
    skip_cancel: &mut bool,
) -> u128 {
    if matched_percent > 0 {
        let trigger_match = 100 / matched_percent;
        if cancel_id % trigger_match == 0 {
            *skip_cancel = true;
            match_price
        } else {
            pre_price
        }
    } else {
        pre_price
    }
}

fn unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_millis() as u64
}

#[derive(Default, Clone, Copy)]
struct PrepStats {
    close_ok: u32,
    close_err: u32,
    cancel_ok: u32,
    cancel_err: u32,
}

impl PrepStats {
    fn merge(&mut self, o: PrepStats) {
        self.close_ok = self.close_ok.saturating_add(o.close_ok);
        self.close_err = self.close_err.saturating_add(o.close_err);
        self.cancel_ok = self.cancel_ok.saturating_add(o.cancel_ok);
        self.cancel_err = self.cancel_err.saturating_add(o.cancel_err);
    }
}

async fn prep_one_account(acc: &Account, market_id: u16) -> anyhow::Result<PrepStats> {
    let mut st = PrepStats::default();
    let api = get_api().await?;
    let rpc = get_rpc().await?;

    let pq = node_runtime::storage()
        .perp_market()
        .user_perp_positions(acc.subaccount.clone());
    if let Some(positions) = api.storage().at_latest().await?.fetch(&pq).await? {
        let mut nonce = unix_ms();
        for p in positions {
            if p.market_id != market_id {
                continue;
            }
            let call = node_runtime::tx().perp_market().close_position(
                acc.subaccount.clone(),
                market_id,
                0,
                None,
            );
            let params = SubstrateExtrinsicParamsBuilder::new().nonce(nonce).build();
            let signed = api.tx().create_partial_offline(&call, params)?.sign(&acc.kp);
            match rpc
                .author_submit_extrinsic(&Bytes::from_owner(signed.into_encoded()))
                .await
            {
                Ok(_) => {
                    st.close_ok = st.close_ok.saturating_add(1);
                    nonce = nonce.saturating_add(1);
                }
                Err(e) => {
                    st.close_err = st.close_err.saturating_add(1);
                    warn!(
                        "准备阶段 平仓失败: user={} subaccount={:?} market={} err={:?}",
                        acc.name, acc.subaccount, market_id, e
                    );
                }
            }
        }
    }

    let oq = node_runtime::storage()
        .perp_market()
        .active_perp_orders_for(acc.subaccount.clone());
    if let Some(res) = api.storage().at_latest().await?.fetch(&oq).await? {
        let mut nonce = unix_ms();
        for (mid, ords) in res {
            for ord in ords {
                let call = node_runtime::tx().perp_market().cancel_order(
                    acc.subaccount,
                    ord.order_id,
                    mid,
                    CancelReason::UserCanceled,
                );
                let params = SubstrateExtrinsicParamsBuilder::new().nonce(nonce).build();
                let signed = api.tx().create_partial_offline(&call, params)?.sign(&acc.kp);
                match rpc
                    .author_submit_extrinsic(&Bytes::from_owner(signed.into_encoded()))
                    .await
                {
                    Ok(_) => {
                        st.cancel_ok = st.cancel_ok.saturating_add(1);
                        nonce = nonce.saturating_add(1);
                    }
                    Err(e) => {
                        st.cancel_err = st.cancel_err.saturating_add(1);
                        warn!(
                            "准备阶段 撤单失败: user={} subaccount={:?} market={} order_id={} err={:?}",
                            acc.name, acc.subaccount, mid, ord.order_id, e
                        );
                    }
                }
            }
        }
    }
    Ok(st)
}

async fn phase_prep(accounts: &[Account], market_id: u16, conc: usize) -> anyhow::Result<PrepStats> {
    let sem = Arc::new(Semaphore::new(conc));
    let mut set = JoinSet::new();
    for acc in accounts {
        let acc = acc.clone();
        let sem = sem.clone();
        set.spawn(async move {
            let _p = sem.acquire().await.map_err(|e| anyhow::anyhow!("{e}"))?;
            prep_one_account(&acc, market_id).await
        });
    }
    let mut totals = PrepStats::default();
    while let Some(r) = set.join_next().await {
        totals.merge(r??);
    }
    Ok(totals)
}

async fn sleep_until_unix_ms(deadline_ms: u64) {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    if deadline_ms > now {
        let d = Duration::from_millis(deadline_ms - now);
        info!("同步起跑：睡眠 {:?} 至 Unix_ms={}", d, deadline_ms);
        tokio::time::sleep(d).await;
    }
}

/// 离线编签 `n` 笔 place/cancel 交替（与 `perp_bench` 单笔逻辑一致，仅去掉 RPC）。
async fn build_account_extrinsics(
    acc: Account,
    market_id: u16,
    order_size: u128,
    matched_percent: u32,
    n: u32,
) -> anyhow::Result<Vec<Bytes>> {
    let api = get_api().await?;
    let mut hf_nonce = unix_ms();
    let mut cancel_id = acc.next_order_id;
    let mut skip_cancel = false;
    let match_price = acc.limit_price;
    let mut out = Vec::with_capacity(n as usize);

    for i in 1u32..=n {
        let is_place = i % 2 == 1;
        if is_place {
            cancel_id = cancel_id.saturating_add(1);
        }
        let tx_nonce = hf_nonce;
        hf_nonce = hf_nonce.saturating_add(1).max(unix_ms());

        let tx_bytes: Bytes = if is_place {
            let price = target_price(
                matched_percent,
                acc.is_long,
                acc.limit_price,
                match_price,
                cancel_id,
                &mut skip_cancel,
            );
            let call = node_runtime::tx().perp_market().place_order(
                acc.subaccount,
                market_id,
                acc.is_long,
                order_size,
                price,
                OrderType::Limit,
                None,
                2,
                None,
                None,
                false,
                PostOnlyParam::None,
            );
            let params = SubstrateExtrinsicParamsBuilder::new().nonce(tx_nonce).build();
            let signed = api
                .tx()
                .create_partial_offline(&call, params)?
                .sign(&acc.kp);
            Bytes::from_owner(signed.into_encoded())
        } else {
            let order_id = cancel_id.saturating_sub(1);
            let call = node_runtime::tx().perp_market().cancel_order(
                acc.subaccount,
                order_id,
                market_id,
                CancelReason::UserCanceled,
            );
            let params = SubstrateExtrinsicParamsBuilder::new().nonce(tx_nonce).build();
            let signed = api
                .tx()
                .create_partial_offline(&call, params)?
                .sign(&acc.kp);
            Bytes::from_owner(signed.into_encoded())
        };
        out.push(tx_bytes);
    }
    Ok(out)
}

async fn pool_worker(
    mut rx: UnboundedReceiver<(Vec<Bytes>, String)>,
    pool_rate: u32,
    throttle_after_sec: u64,
    burst_submit: bool,
) {
    let Ok(rpc) = get_rpc().await else {
        warn!("pool_worker: get_rpc failed");
        return;
    };
    let mut inner_num: u32 = 0;
    let mut call_num: u32 = 0;
    let mut encoded_inner: Vec<u8> = Vec::new();
    // 与 `testnet_test` 池 worker 一致：上一批「允许发下一批」的时间锚（在 RPC 前更新）。
    let mut last_batch_gate = Instant::now();

    async fn flush_extrinsics(
        rpc: &LegacyRpcMethods<EthRuntimeConfig>,
        encoded_inner: &mut Vec<u8>,
        call_num: u32,
        throttle_after_sec: u64,
        burst_submit: bool,
        last_batch_gate: &mut Instant,
    ) {
        if call_num == 0 || encoded_inner.is_empty() {
            return;
        }
        if !burst_submit {
            const MIN_BATCH_INTERVAL: Duration = Duration::from_secs(1);
            let elapsed = last_batch_gate.elapsed();
            if elapsed < MIN_BATCH_INTERVAL {
                tokio::time::sleep(MIN_BATCH_INTERVAL - elapsed).await;
            }
            *last_batch_gate = Instant::now();
        }
        let mut extrinsics: Vec<u8> = Vec::new();
        Compact(call_num).encode_to(&mut extrinsics);
        extrinsics.extend_from_slice(encoded_inner);
        match rpc.author_submit_extrinsics(&extrinsics).await {
            Ok(batch_res) => {
                let got = batch_res.len();
                if got != call_num as usize {
                    if let Some(m) = SUBMIT_ERR_AGG.get() {
                        m.lock()
                            .expect("submit err agg lock")
                            .record_batch_len_mismatch(call_num, got);
                    } else {
                        warn!(
                            "author_submit_extrinsics 返回条数与提交笔数不一致: 提交={call_num} 返回={got}"
                        );
                    }
                }
                for (idx, res) in batch_res.into_iter().enumerate() {
                    match res {
                        Ok(_) => {
                            GLOBAL_SUBMIT_OK.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(e) => {
                            GLOBAL_SUBMIT_ERR.fetch_add(1, Ordering::Relaxed);
                            record_submit_err(
                                &format!("批内 idx={idx} batch_size={call_num}"),
                                &e,
                            );
                        }
                    }
                }
                if got < call_num as usize {
                    let missing = call_num as u64 - got as u64;
                    GLOBAL_SUBMIT_ERR.fetch_add(missing, Ordering::Relaxed);
                }
            }
            Err(e) => {
                let err_s = submit_err_text(&e);
                if is_pool_limit_msg(&err_s) {
                    warn!(
                        "author_submit_extrinsics 整批失败 [pool limit 相关]: batch_size={call_num} err={err_s}"
                    );
                } else {
                    warn!(
                        "author_submit_extrinsics 整批失败: batch_size={call_num} err={err_s}"
                    );
                }
                record_submit_err(&format!("整批 batch_size={call_num}"), &e);
                GLOBAL_SUBMIT_ERR.fetch_add(call_num as u64, Ordering::Relaxed);
            }
        }
        encoded_inner.clear();
        if throttle_after_sec > 0 {
            tokio::time::sleep(Duration::from_secs(throttle_after_sec)).await;
        }
    }

    while let Some((v, _user)) = rx.recv().await {
        inner_num = inner_num.saturating_add(v.len() as u32);
        call_num = call_num.saturating_add(v.len() as u32);
        for b in v {
            encoded_inner.extend_from_slice(&b);
        }
        if inner_num >= pool_rate {
            flush_extrinsics(
                &rpc,
                &mut encoded_inner,
                call_num,
                throttle_after_sec,
                burst_submit,
                &mut last_batch_gate,
            )
            .await;
            inner_num = 0;
            call_num = 0;
        }
    }
    flush_extrinsics(
        &rpc,
        &mut encoded_inner,
        call_num,
        throttle_after_sec,
        burst_submit,
        &mut last_batch_gate,
    )
    .await;
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::Builder::from_default_env()
        .filter_level(log::LevelFilter::Info)
        .init();

    let shard = ShardRunConfig::load()?;
    chain_ws::init(shard.ws_url.clone()).map_err(|e| anyhow::anyhow!("{e}"))?;
    let bench = BenchExtra::from_env()?;
    let quote_ctx = load_market_quote_ctx(bench.market_id).await?;
    info!(
        "发单市场: BENCH_MARKET_ID={} mark_price={} order_size={} tick_size={} price_band=[{}, {}]",
        bench.market_id,
        quote_ctx.mark_price,
        bench.order_size,
        quote_ctx.tick_size,
        quote_ctx.lower,
        quote_ctx.upper
    );
    info!("shard: {}", shard.summary());
    info!(
        "perp_bench_fence: duration={}s scan_conc={} prep_conc={} start_at_ms={:?} skip_prep={} market={} pool_senders={} throttle_after_sec={} burst_submit={} post_chain_scan_sec={} matched%={}",
        bench.duration_sec,
        bench.scan_concurrency,
        bench.prep_concurrency,
        bench.start_at_unix_ms,
        bench.skip_prep,
        bench.market_id,
        bench.pool_senders,
        bench.throttle_sec,
        bench.burst_submit,
        bench.post_chain_scan_sec,
        bench.matched_percent,
    );

    let n_u64 = (shard.rate as u64).saturating_mul(bench.duration_sec);
    let n: u32 = n_u64.min(u32::MAX as u64) as u32;
    if n == 0 {
        anyhow::bail!("RATE×DURATION 结果为 0");
    }
    let places_per_account = n.div_ceil(2);
    info!(
        "栅栏编签：每账户 n={} 笔（RATE×BENCH_DURATION_SEC），共 {} 账户；预估总笔数 {}",
        n,
        shard.account_count,
        (shard.account_count as u64).saturating_mul(n as u64)
    );

    GLOBAL_SUBMIT_OK.store(0, Ordering::Relaxed);
    GLOBAL_SUBMIT_ERR.store(0, Ordering::Relaxed);
    let _ = SUBMIT_ERR_AGG.get_or_init(|| Mutex::new(SubmitErrAgg::default()));
    SubmitErrAgg::reset();

    let rows = derive_accounts(shard.first_addr_index, shard.account_count)?;
    let mut accounts = resolve_accounts_parallel(rows, bench.scan_concurrency).await?;
    accounts = assign_quotes(accounts, bench.market_id).await?;

    if !bench.skip_prep {
        info!("准备阶段：平仓 + 撤挂单（并发 {}）…", bench.prep_concurrency);
        let t_prep = Instant::now();
        let prep_stats = phase_prep(&accounts, bench.market_id, bench.prep_concurrency).await?;
        info!(
            "准备阶段结束，耗时 {:.2}s；RPC 汇总 平仓 ok={} err={}；撤单 ok={} err={}",
            t_prep.elapsed().as_secs_f64(),
            prep_stats.close_ok,
            prep_stats.close_err,
            prep_stats.cancel_ok,
            prep_stats.cancel_err,
        );
        if prep_stats.close_err > 0 || prep_stats.cancel_err > 0 {
            warn!(
                "准备阶段存在失败 RPC（共 {} 笔），请向上翻阅 warn 详情",
                prep_stats
                    .close_err
                    .saturating_add(prep_stats.cancel_err)
            );
        }
    } else {
        info!("已跳过准备阶段（BENCH_SKIP_PREP）");
    }

    if let Some(t) = bench.start_at_unix_ms {
        sleep_until_unix_ms(t).await;
    }

    // ─── 栅栏：并行编签 ─────────────────────────────────────────────
    let t_build = Instant::now();
    let account_scan_keys: Vec<(String, H160)> = accounts
        .iter()
        .map(|a| (a.name.clone(), a.subaccount))
        .collect();
    let sem = Arc::new(Semaphore::new(bench.scan_concurrency));
    let mut build_set = JoinSet::new();
    let n_accounts = accounts.len();
    for (idx, acc) in accounts.into_iter().enumerate() {
        let sem = sem.clone();
        let market_id = bench.market_id;
        let order_size = bench.order_size;
        let matched = bench.matched_percent;
        build_set.spawn(async move {
            let _p = sem.acquire().await.map_err(|e| anyhow::anyhow!("{e}"))?;
            let v = build_account_extrinsics(acc, market_id, order_size, matched, n).await?;
            Ok::<_, anyhow::Error>((idx, v))
        });
    }
    let mut prebuilt: Vec<Option<Vec<Bytes>>> = vec![None; n_accounts];
    while let Some(j) = build_set.join_next().await {
        let (idx, v) = j??;
        prebuilt[idx] = Some(v);
    }
    let prebuilt: Vec<Vec<Bytes>> = prebuilt
        .into_iter()
        .map(|x| x.ok_or_else(|| anyhow::anyhow!("build join internal")))
        .collect::<anyhow::Result<_>>()?;
    info!(
        "栅栏：编签完成，耗时 {:.2}s，开始池化发送",
        t_build.elapsed().as_secs_f64()
    );

    let pre_chain = if bench.post_chain_scan_sec > 0 {
        info!(
            "链上基线：发送前拉取 storage（{} 户，market={}）…",
            account_scan_keys.len(),
            bench.market_id
        );
        Some(
            fetch_chain_stats_parallel(
                &account_scan_keys,
                bench.market_id,
                bench.order_size,
                bench.scan_concurrency,
            )
            .await?,
        )
    } else {
        info!("已跳过链上核对（FENCE_POST_CHAIN_SCAN_SEC=0）");
        None
    };

    // ─── 池化发送（与 testnet_test 思路一致）────────────────────────
    let pool_senders = bench.pool_senders.max(1);
    let pool_rate = ((shard.account_count as u64)
        .saturating_mul(shard.rate as u64)
        .saturating_div(pool_senders as u64))
        .max(1) as u32;
    info!(
        "池参数: pool_senders={} pool_rate={}（累计 {} 笔 extrinsic 后一批 author_submit_extrinsics）；发送节流: {}（与 testnet_test 池一致：每 worker 批间至少 1s，墙钟约 {}s）",
        pool_senders,
        pool_rate,
        pool_rate,
        if bench.burst_submit {
            "已关闭（FENCE_BURST_SUBMIT）"
        } else {
            "每批前至少间隔 1s"
        },
        bench.duration_sec,
    );

    let mut txs: Vec<UnboundedSender<(Vec<Bytes>, String)>> = Vec::new();
    let mut pool_tasks = JoinSet::new();
    for _ in 0..pool_senders {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        txs.push(tx);
        let throttle = bench.throttle_sec;
        let burst = bench.burst_submit;
        pool_tasks.spawn(async move { pool_worker(rx, pool_rate, throttle, burst).await });
    }

    let t_send = Instant::now();
    let rate = shard.rate.max(1) as usize;
    let mut feed_set = JoinSet::new();
    for (acc_idx, vec) in prebuilt.into_iter().enumerate() {
        let pool_tx = txs[acc_idx % pool_senders as usize].clone();
        feed_set.spawn(async move {
            for chunk in vec.chunks(rate) {
                let chunk_vec: Vec<Bytes> = chunk.to_vec();
                if pool_tx.send((chunk_vec, format!("acc_{acc_idx}"))).is_err() {
                    break;
                }
            }
            Ok::<_, anyhow::Error>(())
        });
    }
    while feed_set.join_next().await.is_some() {}

    drop(txs);

    while pool_tasks.join_next().await.is_some() {}

    let ok = GLOBAL_SUBMIT_OK.load(Ordering::Relaxed);
    let err = GLOBAL_SUBMIT_ERR.load(Ordering::Relaxed);
    log_submit_err_summary(ok, err);
    info!(
        "结束: 发送阶段墙钟 {:.2}s；submit_ok={} submit_err={}（RPC 对批量内每条 extrinsic 的返回计数，含 place 与 cancel）",
        t_send.elapsed().as_secs_f64(),
        ok,
        err
    );

    if let Some(pre) = pre_chain {
        let scan_sec = bench.post_chain_scan_sec;
        info!("链上收尾：等待 {}s 后再次扫 storage …", scan_sec);
        tokio::time::sleep(Duration::from_secs(scan_sec)).await;
        let post = fetch_chain_stats_parallel(
            &account_scan_keys,
            bench.market_id,
            bench.order_size,
            bench.scan_concurrency,
        )
        .await?;
        log_chain_reconcile(
            &account_scan_keys,
            &pre,
            &post,
            places_per_account,
            bench.market_id,
            scan_sec,
            ok,
            err,
        );
        diagnose_weak_accounts(
            &account_scan_keys,
            &pre,
            &post,
            places_per_account,
        )
        .await?;
    }
    Ok(())
}
