//! 单实例由 ROOTER 给一段 `addr_idx` 范围内的测试账户子账户充 USDC。
//! 若链上尚无 `user_stats`，会先用 **ROOTER** 做 `quota.activate_account` + `manager_add_quota`（与 `src/main.rs` 里 `create_extra_test_accounts` 一致），
//! 再由该用户签名 `initialize_subaccount`；否则测试网会报 `Invalid signing address`。
//! 多机压测前：先在一台机器上对**全体** `FIRST_ADDR_INDEX` + `ACCOUNT_COUNT` 跑一次（可多次跑，已充会 skip），
//! 完成后各分片机器可跑 `perp_bench` 发压（见 `.env.example`）。

#![allow(missing_docs)]
#![allow(dead_code)]

use bytes::Bytes;
use node_runtime::runtime_types::bounded_collections::bounded_vec::BoundedVec;
use std::str::FromStr;
use std::sync::LazyLock;
use std::time::Duration;
use subxt::backend::rpc::RpcClient;
use subxt::config::substrate::SubstrateExtrinsicParamsBuilder;
use subxt::config::{substrate, SubstrateExtrinsicParams};
use subxt::ext::subxt_core::utils::AccountId20;
use subxt::ext::subxt_rpcs::LegacyRpcMethods;
use subxt::utils::H160;
use subxt::{Config, OnlineClient};
use subxt_signer::eth::Signature;
use subxt_signer::eth::{DerivationPath, Keypair};
use subxt_signer::{bip39, DEV_PHRASE};
use tokio::sync::OnceCell;
use tokio::task::JoinSet;

use log::{debug, info, warn};
use subtx_test::chain_ws;
use subtx_test::shard_run_config::ShardRunConfig;

#[subxt::subxt(
    runtime_metadata_path = "./deepx-node-metadata.scale",
    derive_for_all_types = "Eq, PartialEq, Clone, Debug"
)]
pub mod node_runtime {}

static ROOTER: LazyLock<Keypair> = LazyLock::new(|| {
    let mut sk = [0u8; 32];
    let a = hex::decode("349f7f21d09265b525c562df697cee56d65fe23fe638bb890dd2213a0cca5dcd").unwrap();
    sk.copy_from_slice(&a);
    Keypair::from_secret_key(sk).unwrap()
});

static GLOBAL_API: OnceCell<OnlineClient<EthRuntimeConfig>> = OnceCell::const_new();
static GLOBAL_RPC: OnceCell<RpcClient> = OnceCell::const_new();

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
    Ok(LegacyRpcMethods::<EthRuntimeConfig>::new(rpc_client.clone()))
}

#[derive(Clone)]
struct AccountDetail {
    name: String,
    kp: Keypair,
    subaccount: H160,
    #[allow(dead_code)]
    order_num: u32,
}

/// 轮询建子账户最大次数（与 `INIT_POLL_*` 间隔配合）。
const INIT_POLL_MAX: u32 = 30;
/// 链上只读扫描 `user_stats` / `subaccount_info` 的并发度（避免长时间无日志像卡死）。
const PARALLEL_CHAIN_SCAN: usize = 64;

fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

#[derive(Debug)]
struct RooterTune {
    post_quota_sleep_ms: u64,
    parallel_subaccount_inits: usize,
    init_poll_first_ms: u64,
    init_poll_step_ms: u64,
}

impl RooterTune {
    fn from_env() -> Self {
        Self {
            post_quota_sleep_ms: env_u64("ROOTER_POST_QUOTA_SLEEP_MS", 20).min(2000),
            parallel_subaccount_inits: env_u64("PARALLEL_SUBACCOUNT_INITS", 48)
                .clamp(1, 96) as usize,
            init_poll_first_ms: env_u64("INIT_POLL_FIRST_MS", 45).min(2000),
            init_poll_step_ms: env_u64("INIT_POLL_STEP_MS", 110).min(2000),
        }
    }
}

static ROOTER_TUNE: LazyLock<RooterTune> = LazyLock::new(RooterTune::from_env);

async fn get_subaccount(user: &Keypair) -> anyhow::Result<Vec<H160>> {
    let api = get_api().await?;
    let query = node_runtime::storage()
        .subaccount()
        .user_stats_for(user.public_key().to_account_id().0.into());
    let result = api.storage().at_latest().await?.fetch(&query).await?;

    if let Some(user_stats) = result {
        return Ok(user_stats.subaccounts);
    }

    Err(anyhow::anyhow!("user subaccount not found"))
}

async fn first_subaccount_if_any(user: &Keypair) -> Option<H160> {
    match get_subaccount(user).await {
        Ok(list) => list.first().cloned(),
        Err(_) => None,
    }
}

/// 提交并由用户签名的 `initialize_subaccount`，然后轮询直到出现首个子账户。
async fn submit_init_and_wait_for_subaccount(user: &Keypair, subaccount_label: &str) -> anyhow::Result<H160> {
    debug!("initialize_subaccount({subaccount_label})");
    let api = get_api().await?;
    let rpc = get_rpc().await?;
    let nonce = api
        .tx()
        .account_nonce(&user.public_key().to_account_id())
        .await?;
    let call = node_runtime::tx()
        .subaccount()
        .initialize_subaccount(BoundedVec(subaccount_label.as_bytes().to_vec()));
    let params = SubstrateExtrinsicParamsBuilder::new().nonce(nonce).build();
    let signed_tx = api
        .tx()
        .create_partial_offline(&call, params)?
        .sign(user);
    let call_bytes = Bytes::from_owner(signed_tx.into_encoded());
    match rpc.author_submit_extrinsic(&call_bytes).await {
        Ok(_) => debug!("initialize_subaccount tx accepted: {subaccount_label}"),
        Err(e) => warn!(
            "initialize_subaccount submit ({subaccount_label}): {e:?}（若链上已存在可忽略，继续轮询读链）"
        ),
    }

    let first_ms = ROOTER_TUNE.init_poll_first_ms;
    let step_ms = ROOTER_TUNE.init_poll_step_ms;
    tokio::time::sleep(Duration::from_millis(first_ms)).await;
    if let Some(h) = first_subaccount_if_any(user).await {
        return Ok(h);
    }
    for attempt in 2..=INIT_POLL_MAX {
        tokio::time::sleep(Duration::from_millis(step_ms)).await;
        if let Some(h) = first_subaccount_if_any(user).await {
            debug!("{subaccount_label}: subaccount visible after {attempt} polls");
            return Ok(h);
        }
    }

    anyhow::bail!(
        "initialize_subaccount 后仍查不到子账户: {subaccount_label}（检查配额激活、RPC、链上 subaccount pallet）"
    )
}

async fn deposit(
    kp: &Keypair,
    subaccount: &H160,
    market_id: u8,
    asset: &str,
    amount: u128,
    nonce: &mut u64,
) -> anyhow::Result<()> {
    let api = get_api().await?;
    let rpc = get_rpc().await?;

    let call = node_runtime::tx().lending().deposit(
        None,
        *subaccount,
        market_id,
        BoundedVec(asset.as_bytes().to_vec()),
        amount,
    );
    let params = SubstrateExtrinsicParamsBuilder::new().nonce(*nonce).build();
    let signed_tx = api.tx().create_partial_offline(&call, params)?.sign(kp);
    let call_bytes = Bytes::from_owner(signed_tx.into_encoded());

    let signer_id = kp.public_key().to_account_id();
    match rpc.author_submit_extrinsic(&call_bytes).await {
        Ok(_) => {
            debug!(
                "deposit submitted: owner={:?}, subaccount={:?}, market_id={}, asset={}, amount={}",
                hex::encode(&kp.public_key().to_account_id().0),
                subaccount,
                market_id,
                asset,
                amount
            );
            // 与历史 `testnet_sharded::deposit` 一致：该路径上 ROOTER 单笔 deposit 后本地游标前进 2。
            *nonce = nonce.saturating_add(2);
        }
        Err(e) => {
            warn!("Failed to deposit for subaccount {:?}: {:?}", subaccount, e);
            *nonce = api.tx().account_nonce(&signer_id).await.unwrap_or(*nonce);
        }
    }
    Ok(())
}

async fn check_deposit(account: &AccountDetail) -> anyhow::Result<bool> {
    let api = get_api().await?;
    let position_query = node_runtime::storage().lending().positions_for(
        AccountId20 {
            0: account.subaccount.clone().0,
        },
        1,
    );
    let positions = api.storage().at_latest().await?.fetch(&position_query).await?;
    let skip_deposit = if let Some(positions) = positions {
        !positions.deposits.is_empty()
    } else {
        false
    };
    Ok(skip_deposit)
}

async fn create_sharded_test_accounts(
    first_addr_index: u32,
    count: u32,
) -> anyhow::Result<Vec<AccountDetail>> {
    info!("rooter_deposit: creating key list first_addr_index={first_addr_index}, count={count}");
    let mut result = Vec::new();
    let _client = get_api().await?;
    let _rpc = get_rpc().await?;

    for i in 0..count {
        let addr_idx = first_addr_index + i;
        let kp = Keypair::from_phrase(
            &bip39::Mnemonic::from_str(DEV_PHRASE)?,
            None,
            DerivationPath::eth(0, addr_idx),
        )?;
        let name = format!("test_user_{addr_idx}");
        debug!(
            "[{i}] account init, address: {:?}",
            hex::encode(&kp.public_key().to_account_id().0)
        );
        result.push(AccountDetail {
            name,
            kp,
            subaccount: Default::default(),
            order_num: 0,
        });
    }
    Ok(result)
}

const DEPOSIT_AMOUNT: u128 = 100_000_000;
const INIT_QUOTA: u32 = 429467295;

/// 未激活的 EVM 账户无法签原生 pallet 调用；由 ROOTER 代提 `activate_account` 与 `manager_add_quota`。
/// 与 `main.rs::create_extra_test_accounts` 相同：`let mut nonce = api.tx().account_nonce(ROOTER)`，仅 **`submit` 成功时 `nonce += 1`**；失败重试前重新读链上 nonce。本函数不接收、不修改 `rooter_deposit::main` 中的 `root_nonce`。
async fn ensure_quota_for_test_user(root_kp: &Keypair, user: &Keypair) -> anyhow::Result<()> {
    let api = get_api().await?;
    let rpc = get_rpc().await?;
    let aid = user.public_key().to_account_id();
    let root_id = root_kp.public_key().to_account_id();

    let mut nonce = api.tx().account_nonce(&root_id).await?;

    for attempt in 0u32..4 {
        let call = node_runtime::tx().quota().activate_account(aid.clone());
        let params = SubstrateExtrinsicParamsBuilder::new().nonce(nonce).build();
        let signed_tx = api.tx().create_partial_offline(&call, params)?.sign(root_kp);
        let call_bytes = Bytes::from_owner(signed_tx.into_encoded());
        match rpc.author_submit_extrinsic(&call_bytes).await {
            Ok(_) => {
                nonce += 1;
                debug!("activate_account ok for {:?}", aid);
                break;
            }
            Err(e) => {
                let esl = format!("{e:?}").to_lowercase();
                if esl.contains("alreadyactivated") || esl.contains("already") {
                    debug!("activate_account skip (already): {:?}", aid);
                    break;
                }
                warn!("Error submitting activate_account: {e:?}");
                if attempt + 1 >= 4 {
                    anyhow::bail!(
                        "activate_account 在 4 次尝试后仍未成功（{:?}）；未激活则后续 initialize_subaccount 会报 Invalid signing address",
                        aid
                    );
                }
                nonce = api.tx().account_nonce(&root_id).await?;
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
    }

    for mgr_try in 0u32..5 {
        let call = node_runtime::tx()
            .quota()
            .manager_add_quota(aid.clone(), INIT_QUOTA);
        let params = SubstrateExtrinsicParamsBuilder::new().nonce(nonce).build();
        let signed_tx = api.tx().create_partial_offline(&call, params)?.sign(root_kp);
        let call_bytes = Bytes::from_owner(signed_tx.into_encoded());
        match rpc.author_submit_extrinsic(&call_bytes).await {
            Ok(_) => {
                nonce += 1;
                debug!("manager_add_quota ok");
                break;
            }
            Err(e) => {
                let esl = format!("{e:?}").to_lowercase();
                if esl.contains("already")
                    || esl.contains("duplicate")
                    || esl.contains("exist")
                {
                    debug!("manager_add_quota 视为已满足（链上提示）: {e:?}");
                    break;
                }
                warn!("Error submitting add quota for account: {e:?}");
                if mgr_try + 1 >= 5 {
                    anyhow::bail!(
                        "manager_add_quota 在 5 次尝试后仍未成功（{:?}）；未加配额则后续 initialize_subaccount 可能报 Invalid signing address",
                        aid
                    );
                }
                nonce = api.tx().account_nonce(&root_id).await?;
                tokio::time::sleep(Duration::from_millis(400)).await;
            }
        }
    }

    tokio::time::sleep(Duration::from_millis(ROOTER_TUNE.post_quota_sleep_ms)).await;
    let _ = nonce;
    Ok(())
}

#[tokio::main(flavor = "multi_thread", worker_threads = 8)]
async fn main() -> anyhow::Result<()> {
    env_logger::init();
    let cfg = ShardRunConfig::load()?;
    info!("rooter_deposit {}", cfg.summary());
    info!(
        "rooter_deposit 性能参数（可用环境变量覆盖，见 crate 顶部说明）: {:?}",
        &*ROOTER_TUNE
    );
    chain_ws::init(cfg.ws_url.clone()).map_err(|e| anyhow::anyhow!("{e}"))?;

    tokio::time::sleep(Duration::from_millis(2000)).await;

    let mut accounts = create_sharded_test_accounts(cfg.first_addr_index, cfg.account_count).await?;

    let root_kp = ROOTER.clone();
    let api = get_api().await?;
    let root_id = root_kp.public_key().to_account_id();
    let mut root_nonce = api.tx().account_nonce(&root_id).await?;
    info!(
        "ROOTER Substrate nonce 起点 = {}（链上 account_nonce）",
        root_nonce
    );

    /// 断线重跑：若 **quota 已整段跑完**、只需从「并行 initialize_subaccount」接着跑，改为 `true` 以跳过 ROOTER `activate` / `manager_add_quota` 循环。
    /// **仍会执行上面的链上扫描**，以正确划分 `need_init`。整段 `rooter_deposit` 跑通后请改回 `false`。
    const RESUME_SKIP_ROOTER_QUOTA: bool = false;

    let n = accounts.len();
    info!(
        "链上扫描子账户是否存在（{} 个账户，每批并发 {}）…",
        n,
        PARALLEL_CHAIN_SCAN
    );
    let mut first_sub: Vec<Option<H160>> = vec![None; n];
    for chunk_start in (0..n).step_by(PARALLEL_CHAIN_SCAN) {
        let end = (chunk_start + PARALLEL_CHAIN_SCAN).min(n);
        let mut set = JoinSet::new();
        for i in chunk_start..end {
            let kp = accounts[i].kp.clone();
            set.spawn(async move {
                let sub = first_subaccount_if_any(&kp).await;
                Ok::<_, anyhow::Error>((i, sub))
            });
        }
        while let Some(joined) = set.join_next().await {
            let (i, sub) = joined.map_err(|e| anyhow::anyhow!("扫描任务 join: {e}"))??;
            first_sub[i] = sub;
        }
        info!("扫描进度 {}/{}", end, n);
    }

    let mut already_have_sub: Vec<(usize, H160)> = Vec::new();
    let mut need_init: Vec<(usize, Keypair, String)> = Vec::new();
    for i in 0..n {
        if let Some(subaccount) = first_sub[i].clone() {
            already_have_sub.push((i, subaccount));
        } else {
            need_init.push((
                i,
                accounts[i].kp.clone(),
                accounts[i].name.clone(),
            ));
        }
    }

    if !already_have_sub.is_empty() {
        info!(
            "拉取已有子账户的 subaccount_info（{} 条，每批并发 {}）…",
            already_have_sub.len(),
            PARALLEL_CHAIN_SCAN
        );
        for chunk in already_have_sub.chunks(PARALLEL_CHAIN_SCAN) {
            let mut set = JoinSet::new();
            for (i, subaccount) in chunk {
                let i = *i;
                let subaccount = subaccount.clone();
                let name = accounts[i].name.clone();
                let kp = accounts[i].kp.clone();
                set.spawn(async move {
                    let subaccount_info_query =
                        node_runtime::storage().subaccount().subaccount_info(subaccount.clone());
                    let subaccount_info = get_api()
                        .await?
                        .storage()
                        .at_latest()
                        .await?
                        .fetch(&subaccount_info_query)
                        .await?
                        .ok_or_else(|| anyhow::anyhow!("subaccount_info missing for {:?}", name))?;
                    Ok::<_, anyhow::Error>((
                        i,
                        AccountDetail {
                            name,
                            kp,
                            subaccount,
                            order_num: subaccount_info.next_order_id.saturating_sub(1),
                        },
                    ))
                });
            }
            while let Some(joined) = set.join_next().await {
                let (i, detail) = joined.map_err(|e| anyhow::anyhow!("subaccount_info join: {e}"))??;
                accounts[i] = detail;
            }
        }
    }

    let n_need = need_init.len();
    let par_init = ROOTER_TUNE.parallel_subaccount_inits;
    info!(
        "子账户: 链上已有 {} 个，需新建 {} 个（initialize 并行度 {}）",
        accounts.len() - n_need,
        n_need,
        par_init
    );

    if n_need > 0 {
        info!(
            "ROOTER activate + manager_add_quota：每个待建账户 2 笔、必须串行（ROOTER 单 nonce 流），共 {} 个账户（约 {} 笔 submit）；post_quota_sleep_ms={}",
            n_need,
            n_need.saturating_mul(2),
            ROOTER_TUNE.post_quota_sleep_ms
        );
        info!("若此阶段过慢，属 RPC/出块节奏限制；已尽量压低每笔后的 sleep，可用 ROOTER_POST_QUOTA_SLEEP_MS 再调");

        if RESUME_SKIP_ROOTER_QUOTA {
            info!(
                "RESUME_SKIP_ROOTER_QUOTA：跳过 activate/manager_add_quota；仅对齐 ROOTER nonce 后进入 initialize_subaccount"
            );
            root_nonce = api.tx().account_nonce(&root_id).await?;
            info!("对齐后 ROOTER nonce = {}（account_nonce）", root_nonce);
        } else {
            root_nonce = api.tx().account_nonce(&root_id).await?;
            info!(
                "quota 开始前 ROOTER nonce = {}（account_nonce，避免长扫描后游标过时）",
                root_nonce
            );
            for (qi, (_, kp, _)) in need_init.iter().enumerate() {
                if qi == 0 || (qi + 1) % 25 == 0 || qi + 1 == n_need {
                    info!("quota 进度 {}/{}", qi + 1, n_need);
                }
                ensure_quota_for_test_user(&root_kp, kp).await?;
            }
            root_nonce = api.tx().account_nonce(&root_id).await?;
            info!(
                "quota 串行结束，ROOTER nonce 已按链上对齐 -> {}（供后续 deposit）",
                root_nonce
            );
        }
    }
    if !need_init.is_empty() {
        if RESUME_SKIP_ROOTER_QUOTA {
            info!("RESUME：已跳过 quota，等待 400ms 后并行 initialize_subaccount");
        } else {
            info!("quota 阶段结束，等待 400ms 后并行 initialize_subaccount");
        }
        tokio::time::sleep(Duration::from_millis(400)).await;
    }

    let total_init_batches = (n_need + par_init - 1) / par_init;
    let mut batch_idx = 0u32;
    for chunk in need_init.chunks(par_init) {
        batch_idx += 1;
        info!(
            "initialize_subaccount 并行批 {}/{}（本批 {} 个）",
            batch_idx,
            total_init_batches.max(1),
            chunk.len()
        );
        let mut set = JoinSet::new();
        for (idx, kp, name) in chunk {
            let kp = kp.clone();
            let name = name.clone();
            let idx = *idx;
            set.spawn(async move {
                let sub = submit_init_and_wait_for_subaccount(&kp, &name).await?;
                Ok::<_, anyhow::Error>((idx, sub))
            });
        }
        let mut init_out: Vec<(usize, H160)> = Vec::with_capacity(chunk.len());
        while let Some(joined) = set.join_next().await {
            init_out.push(joined.map_err(|e| anyhow::anyhow!("并行任务 join: {e}"))??);
        }
        let mut fetch_set = JoinSet::new();
        for (idx, sub) in init_out {
            fetch_set.spawn(async move {
                let subaccount_info_query =
                    node_runtime::storage().subaccount().subaccount_info(sub.clone());
                let subaccount_info = get_api()
                    .await?
                    .storage()
                    .at_latest()
                    .await?
                    .fetch(&subaccount_info_query)
                    .await?
                    .ok_or_else(|| anyhow::anyhow!("subaccount_info missing for index {idx}"))?;
                Ok::<_, anyhow::Error>((
                    idx,
                    sub,
                    subaccount_info.next_order_id.saturating_sub(1),
                ))
            });
        }
        let mut finished_in_batch = 0u32;
        while let Some(joined) = fetch_set.join_next().await {
            let (idx, sub, order_num) =
                joined.map_err(|e| anyhow::anyhow!("subaccount_info 并行拉取 join: {e}"))??;
            let (name, kp) = (accounts[idx].name.clone(), accounts[idx].kp.clone());
            accounts[idx] = AccountDetail {
                name,
                kp,
                subaccount: sub,
                order_num,
            };
            finished_in_batch += 1;
        }
        info!(
            "并行批 {}/{} 内 {} 个子账户已写入 accounts",
            batch_idx,
            total_init_batches.max(1),
            finished_in_batch
        );
    }

    let n_acct = accounts.len();
    info!(
        "ROOTER lending deposit：先并行检查 positions（{} 个账户，每批并发 {}），再串行 submit（ROOTER 单 nonce，无法并行上链）",
        n_acct,
        PARALLEL_CHAIN_SCAN
    );
    let mut need_deposit: Vec<usize> = Vec::new();
    for chunk_start in (0..n_acct).step_by(PARALLEL_CHAIN_SCAN) {
        let end = (chunk_start + PARALLEL_CHAIN_SCAN).min(n_acct);
        let mut set = JoinSet::new();
        for i in chunk_start..end {
            let x = accounts[i].clone();
            set.spawn(async move {
                let skip = check_deposit(&x).await?;
                Ok::<_, anyhow::Error>((i, skip))
            });
        }
        while let Some(joined) = set.join_next().await {
            let (i, skip) = joined.map_err(|e| anyhow::anyhow!("deposit 检查任务 join: {e}"))??;
            if !skip {
                need_deposit.push(i);
            }
        }
        info!("deposit positions 检查进度 {}/{}", end, n_acct);
    }
    need_deposit.sort_unstable();
    let skipped = (n_acct - need_deposit.len()) as u32;
    info!(
        "positions 检查结束：将新发起 deposit {} 笔，跳过已充 {} 笔；串行 submit（ROOTER 单 nonce，整体耗时主要取决于 RPC/池子）…",
        need_deposit.len(),
        skipped
    );

    let mut done = 0u32;
    let n_dep = need_deposit.len();
    for (di, &i) in need_deposit.iter().enumerate() {
        let x = &accounts[i];
        if di == 0 || (di + 1) % 50 == 0 || di + 1 == n_dep {
            info!(
                "deposit submit 进度 {}/{}（当前 {}）",
                di + 1,
                n_dep,
                x.name
            );
        }
        debug!("ROOTER deposit -> {:?} ({})", x.subaccount, x.name);
        deposit(
            &root_kp,
            &x.subaccount,
            1,
            "usdc",
            DEPOSIT_AMOUNT,
            &mut root_nonce,
        )
        .await?;
        done += 1;
    }

    info!(
        "rooter_deposit 完成: 新发起存款约 {} 笔，跳过已充 {} 笔，共 {} 个账户",
        done,
        skipped,
        accounts.len()
    );
    Ok(())
}
