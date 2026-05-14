//! # 永续压测 — **栅栏 + 池化批量提交**（`perp_bench_fence`）
//!
//! 与 `perp_bench` **共用**环境变量与前置流程（`ShardRunConfig::load`、`BENCH_*`、准备阶段等），**不修改** `perp_bench.rs`。
//!
//! ## 与 `perp_bench` 的差异
//!
//! 1. **编签阶段**：每账户预先构造 `n = RATE * BENCH_DURATION_SEC` 笔 **place / cancel 交替** 的已签名 payload（nonce 仍为时间戳底 + 递增，与 `perp_bench` 一致）。
//! 2. **栅栏**：所有账户编签任务 **全部完成后** 才进入发送阶段（`JoinSet` 自然形成栅栏）。
//! 3. **发送阶段**：`FENCE_POOL_SENDERS` 个池 worker 从 channel 收 **`RATE` 笔为一块`** 的 `Vec<Bytes>`，凑满 `pool_rate` 笔后调用 **`author_submit_extrinsics`**（格式与 `testnet_test` 一致：`Compact(笔数)` 前缀 + 裸编码拼接）。**批间节流以铺满 `BENCH_DURATION_SEC`**：与 `testnet_test` 池化路径一致，每个 worker 在**每一批** RPC 前若距上一批不足 **1 秒**则 `sleep`，使全网约 `ACCOUNT_COUNT×RATE` 笔/秒、总墙钟约 **`BENCH_DURATION_SEC`**（`FENCE_BURST_SUBMIT=1` 可关闭节流、恢复瞬时灌满）。
//!
//! ## 内存与风险
//!
//! 内存约 **账户数 × n × 单笔编码大小**；大账户数、长 `BENCH_DURATION_SEC` 时请自行评估机器 RAM。编签并发受 **`CHAIN_SCAN_CONCURRENCY`** 限制，以降低瞬时 CPU/内存尖峰。
//!
//! ## 额外环境变量
//!
//! | 变量 | 含义 | 默认 |
//! |------|------|------|
//! | `FENCE_POOL_SENDERS` | 池 worker 数量（与 `testnet_test` 的 `pool_sender_num` 同角色） | `20` |
//! | `FENCE_THROTTLE_SEC` | 每批 RPC **之后**额外睡眠秒数（默认 `0`） | `0` |
//! | `FENCE_BURST_SUBMIT` | 设为 `1`/`true` 时**关闭**批间 1s 节流（旧行为：尽快发完） | 关 |
//!
//! ```bash
//! RUST_LOG=info cargo run --release --bin perp_bench_fence
//! ```

#![allow(missing_docs)]

use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
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
use subtx_test::shard_run_config::ShardRunConfig;

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
}

fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

fn env_truthy(key: &str) -> bool {
    matches!(
        std::env::var(key)
            .unwrap_or_default()
            .to_ascii_lowercase()
            .as_str(),
        "1" | "true" | "yes" | "on"
    )
}

impl BenchExtra {
    fn from_env() -> anyhow::Result<Self> {
        let matched_percent: u32 = std::env::var("BENCH_MATCHED_PERCENT")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        if matched_percent > 100 {
            anyhow::bail!("BENCH_MATCHED_PERCENT 必须在 0..=100");
        }
        Ok(Self {
            duration_sec: env_u64("BENCH_DURATION_SEC", 60).max(1),
            scan_concurrency: env_u64("CHAIN_SCAN_CONCURRENCY", 64).clamp(1, 512) as usize,
            prep_concurrency: env_u64("BENCH_PREP_CONCURRENCY", 32).clamp(1, 256) as usize,
            start_at_unix_ms: std::env::var("BENCH_START_AT_UNIX_MS")
                .ok()
                .and_then(|s| s.parse().ok()),
            skip_prep: env_truthy("BENCH_SKIP_PREP"),
            market_id: env_u64("BENCH_MARKET_ID", 3).min(u16::MAX as u64) as u16,
            order_size: env_u64("BENCH_ORDER_SIZE", 1_000_000_000_000_000) as u128,
            matched_percent,
            pool_senders: env_u64("FENCE_POOL_SENDERS", 20).clamp(1, 256) as u32,
            throttle_sec: env_u64("FENCE_THROTTLE_SEC", 0).min(3600),
            burst_submit: env_truthy("FENCE_BURST_SUBMIT"),
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

async fn load_mark_price(market_id: u16) -> anyhow::Result<u128> {
    let api = get_api().await?;
    let q = node_runtime::storage().perp_market().perp_markets(market_id);
    let m = api
        .storage()
        .at_latest()
        .await?
        .fetch(&q)
        .await?
        .ok_or_else(|| anyhow::anyhow!("market {market_id} 不存在"))?;
    Ok(m.mark_price)
}

fn prune_mark(p: u128) -> u128 {
    let r = p.checked_rem_euclid(100_000).unwrap_or(0);
    p.saturating_sub(r)
}

async fn assign_quotes(mut accounts: Vec<Account>, market_id: u16) -> anyhow::Result<Vec<Account>> {
    let tick: u128 = 10_000;
    let mark = load_mark_price(market_id).await?;
    let mark = prune_mark(mark);
    for (i, a) in accounts.iter_mut().enumerate() {
        a.is_long = i % 2 == 0;
        let off = tick.saturating_mul((i as u128).saturating_add(1));
        a.limit_price = if a.is_long {
            mark.saturating_sub(off)
        } else {
            mark.saturating_add(off)
        };
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
                for res in batch_res {
                    match res {
                        Ok(_) => {
                            GLOBAL_SUBMIT_OK.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(_) => {
                            GLOBAL_SUBMIT_ERR.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
            }
            Err(e) => {
                warn!("author_submit_extrinsics: {:?}", e);
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
    info!("shard: {}", shard.summary());
    info!(
        "perp_bench_fence: duration={}s scan_conc={} prep_conc={} start_at_ms={:?} skip_prep={} market={} pool_senders={} throttle_after_sec={} burst_submit={} matched%={}",
        bench.duration_sec,
        bench.scan_concurrency,
        bench.prep_concurrency,
        bench.start_at_unix_ms,
        bench.skip_prep,
        bench.market_id,
        bench.pool_senders,
        bench.throttle_sec,
        bench.burst_submit,
        bench.matched_percent,
    );

    let n_u64 = (shard.rate as u64).saturating_mul(bench.duration_sec);
    let n: u32 = n_u64.min(u32::MAX as u64) as u32;
    if n == 0 {
        anyhow::bail!("RATE×DURATION 结果为 0");
    }
    info!(
        "栅栏编签：每账户 n={} 笔（RATE×BENCH_DURATION_SEC），共 {} 账户；预估总笔数 {}",
        n,
        shard.account_count,
        (shard.account_count as u64).saturating_mul(n as u64)
    );

    GLOBAL_SUBMIT_OK.store(0, Ordering::Relaxed);
    GLOBAL_SUBMIT_ERR.store(0, Ordering::Relaxed);

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
    info!(
        "结束: 发送阶段墙钟 {:.2}s；submit_ok={} submit_err={}（批量 RPC 计数，与单笔 perp_bench 口径可能不同）",
        t_send.elapsed().as_secs_f64(),
        ok,
        err
    );
    Ok(())
}
