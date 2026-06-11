//! # 永续压测（`perp_bench`）
//!
//! 阶段：**配置 → 派生账户 → 并发解析子账户 →（可选）准备 →（可选）墙钟对齐 → 按账户并发发压**。
//! 多机：各机配置不同 `FIRST_ADDR_INDEX` / `ACCOUNT_COUNT`，并共用同一`BENCH_START_AT_UNIX_MS` 即可近似同时起跑。
//!
//! ```bash
//! RUST_LOG=info cargo run --release --bin perp_bench
//! ```
//!
//! ## 环境变量（`ShardRunConfig::load()` 以加载 dotenv）
//!
//! | 变量 | 含义 | 默认 |
//! |------|------|------|
//! | `BENCH_DURATION_SEC` | 发压持续秒数 | `60` |
//! | `CHAIN_SCAN_CONCURRENCY` | 解析子账户时每批并发数 | `64` |
//! | `BENCH_PREP_CONCURRENCY` | 平仓+撤单并发度 | `32` |
//! | `BENCH_START_AT_UNIX_MS` | 墙钟对齐起跑（Unix 毫秒），不设则立即 | 无 |
//! | `BENCH_SKIP_PREP` | `1` 跳过平仓与撤挂单 | `0` |
//! | `BENCH_MARKET_ID` | 永续 `market_id` | `3` |
//! | `BENCH_ORDER_SIZE` | `place_order` size | `1000000000000000` |
//! | `BENCH_MATCHED_PERCENT` | 与 `testnet_test` 中 `MATCHED_PERCENT` 同义 | `0` |

#![allow(missing_docs)]

use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use subxt::backend::rpc::RpcClient;
use subxt::config::substrate::SubstrateExtrinsicParamsBuilder;
use subxt::config::{substrate, SubstrateExtrinsicParams, Config};
use subxt::ext::subxt_core::utils::AccountId20;
use subxt::ext::subxt_rpcs::LegacyRpcMethods;
use subxt::utils::H160;
use subxt::OnlineClient;
use subxt_signer::eth::Signature;
use subxt_signer::eth::{DerivationPath, Keypair};
use subxt_signer::{bip39, DEV_PHRASE};
use tokio::sync::{OnceCell, Semaphore};
use tokio::task::JoinSet;
use tokio::time::MissedTickBehavior;

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

// ─── runtime ─────────────────────────────────────────────────────────

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

// ─── bench env ─────────────────────────────────────────────────────

struct BenchExtra {
    duration_sec: u64,
    scan_concurrency: usize,
    prep_concurrency: usize,
    start_at_unix_ms: Option<u64>,
    skip_prep: bool,
    market_id: u16,
    order_size: u128,
    matched_percent: u32,
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
        })
    }
}

// ─── model ───────────────────────────────────────────────────────────

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
        "限价分配 market={}: mark={} tick={} band=[{}, {}]",
        market_id, ctx.mark_price, ctx.tick_size, ctx.lower, ctx.upper
    );
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

// ─── prep（与 `testnet_test::clear_exist_positions` / `cancel_pre_orders` 相同）──

async fn prep_one_account(acc: &Account, market_id: u16) -> anyhow::Result<()> {
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
            info!(
                "{} subaccount: {:?} close perp position for market: {}",
                acc.name, acc.subaccount, p.market_id
            );
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
                    nonce = nonce.saturating_add(1);
                }
                Err(e) => {
                    warn!("{} Failed to close perp position: {:?}", acc.name, e);
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
                        info!(
                            "{:?} cancel_pre_order {} success",
                            acc.subaccount, ord.order_id
                        );
                        nonce = nonce.saturating_add(1);
                    }
                    Err(e) => {
                        warn!("{} Failed to cancel_pre_orders: {:?}", acc.name, e);
                    }
                }
            }
        }
    }
    Ok(())
}

async fn phase_prep(accounts: &[Account], market_id: u16, conc: usize) -> anyhow::Result<()> {
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
    while let Some(r) = set.join_next().await {
        r??;
    }
    Ok(())
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

// ─── load：place / cancel 交错；nonce 用 Unix 毫秒底 + 递增（见文件头）──

async fn account_stream_place_cancel(
    acc: Account,
    market_id: u16,
    order_size: u128,
    rate: u32,
    matched_percent: u32,
    deadline: Instant,
) {
    let spacing = Duration::from_secs_f64(1.0 / rate.max(1) as f64);
    let mut tick = tokio::time::interval(spacing);
    tick.set_missed_tick_behavior(MissedTickBehavior::Delay);

    let api = match get_api().await {
        Ok(a) => a,
        Err(e) => {
            warn!("{} get_api: {e:?}", acc.name);
            return;
        }
    };
    let rpc = match get_rpc().await {
        Ok(r) => r,
        Err(e) => {
            warn!("{} get_rpc: {e:?}", acc.name);
            return;
        }
    };

    // 与 `main.rs::place_order_no_wait_response_evm` 一致：时间戳 nonce，不用 account_nonce。
    let mut hf_nonce = unix_ms();

    let mut cancel_id = acc.next_order_id;
    let mut i: u64 = 1;
    let mut skip_cancel = false;
    let match_price = acc.limit_price;

    while Instant::now() < deadline {
        tick.tick().await;

        let is_place = i % 2 == 1;
        if is_place {
            cancel_id = cancel_id.saturating_add(1);
        }

        let tx_nonce = hf_nonce;
        hf_nonce = hf_nonce.saturating_add(1).max(unix_ms());

        let tx_bytes: Bytes = match if is_place {
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
            api.tx()
                .create_partial_offline(&call, params)
                .map(|mut p| Bytes::from_owner(p.sign(&acc.kp).into_encoded()))
        } else {
            let order_id = cancel_id.saturating_sub(1);
            let call = node_runtime::tx().perp_market().cancel_order(
                acc.subaccount,
                order_id,
                market_id,
                CancelReason::UserCanceled,
            );
            let params = SubstrateExtrinsicParamsBuilder::new().nonce(tx_nonce).build();
            api.tx()
                .create_partial_offline(&call, params)
                .map(|mut p| Bytes::from_owner(p.sign(&acc.kp).into_encoded()))
        } {
            Ok(b) => b,
            Err(e) => {
                warn!("{} build tx: {e:?}", acc.name);
                if is_place {
                    cancel_id = cancel_id.saturating_sub(1);
                }
                continue;
            }
        };

        match rpc.author_submit_extrinsic(&tx_bytes).await {
            Ok(_) => {
                GLOBAL_SUBMIT_OK.fetch_add(1, Ordering::Relaxed);
                i += 1;
            }
            Err(e) => {
                GLOBAL_SUBMIT_ERR.fetch_add(1, Ordering::Relaxed);
                warn!("{} submit: {:?}", acc.name, e);
                if is_place {
                    cancel_id = cancel_id.saturating_sub(1);
                }
            }
        }
    }
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
        "perp_bench: duration={}s scan_conc={} prep_conc={} start_at_ms={:?} skip_prep={} market={} order_size={} matched%={}",
        bench.duration_sec,
        bench.scan_concurrency,
        bench.prep_concurrency,
        bench.start_at_unix_ms,
        bench.skip_prep,
        bench.market_id,
        bench.order_size,
        bench.matched_percent,
    );

    GLOBAL_SUBMIT_OK.store(0, Ordering::Relaxed);
    GLOBAL_SUBMIT_ERR.store(0, Ordering::Relaxed);

    let rows = derive_accounts(shard.first_addr_index, shard.account_count)?;
    let mut accounts = resolve_accounts_parallel(rows, bench.scan_concurrency).await?;
    accounts = assign_quotes(accounts, bench.market_id).await?;
    //准备阶段
    if !bench.skip_prep {
        info!("准备阶段：平仓 + 撤挂单（并发 {}）…", bench.prep_concurrency);
        let t_prep = Instant::now();
        phase_prep(&accounts, bench.market_id, bench.prep_concurrency).await?;
        info!(
            "准备阶段结束，耗时 {:.2}s；即将发压 {}s",
            t_prep.elapsed().as_secs_f64(),
            bench.duration_sec
        );
    } else {
        info!("已跳过准备阶段（BENCH_SKIP_PREP）");
    }

    if let Some(t) = bench.start_at_unix_ms {
        sleep_until_unix_ms(t).await;
    }
    //发压
    let deadline = Instant::now() + Duration::from_secs(bench.duration_sec);
    let t0 = Instant::now();
    let mut set = JoinSet::new();
    for acc in accounts {
        let market_id = bench.market_id;
        let order_size = bench.order_size;
        let rate = shard.rate;
        let matched = bench.matched_percent;
        set.spawn(async move {
            account_stream_place_cancel(acc, market_id, order_size, rate, matched, deadline).await;
        });
    }
    while set.join_next().await.is_some() {}

    let elapsed = t0.elapsed().as_secs_f64().max(1e-6);
    let ok = GLOBAL_SUBMIT_OK.load(Ordering::Relaxed);
    let err = GLOBAL_SUBMIT_ERR.load(Ordering::Relaxed);
    info!(
        "结束: submit_ok={} submit_err={} wall={:.2}s 平均提交 {:.1} tx/s",
        ok,
        err,
        elapsed,
        ok as f64 / elapsed
    );
    Ok(())
}
