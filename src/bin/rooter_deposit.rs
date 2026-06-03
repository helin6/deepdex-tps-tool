//! ROOTER 为 `FIRST_ADDR_INDEX`..`+ACCOUNT_COUNT` 测试账户：quota 激活 → `initialize_subaccount` → USDC 存款。
//! Multicall 见 `ROOT_DEPOSIT_MULTICALL`；环境变量见 `.env.example`。
//!
//! 编译：`cargo run --bin rooter_deposit`

#![allow(missing_docs)]
#![allow(dead_code)]

use bytes::Bytes;
use ethereum::{EIP1559Transaction as EthEip1559, TransactionAction as EthTxAction};
use node_runtime::runtime_types::bounded_collections::bounded_vec::BoundedVec;
use std::str::FromStr;
use std::sync::LazyLock;
use std::time::Duration;
use subxt::config::substrate::SubstrateExtrinsicParamsBuilder;
use subxt::config::{substrate, SubstrateExtrinsicParams};
use subxt::ext::subxt_core::utils::AccountId20;
use subxt::ext::subxt_rpcs::LegacyRpcMethods;
use subxt::utils::H160;
use subxt::{Config, OnlineClient};
use subxt_signer::eth::Signature;
use subxt_signer::eth::{DerivationPath, Keypair};
use subxt_signer::{bip39, DEV_PHRASE};
use tokio::sync::Mutex;
use tokio::task::JoinSet;

use log::{info, warn};
use subtx_test::chain_ws;
use subtx_test::evm_rpc::{self, EvmReceipt};
use subtx_test::multicall_deposit::encode_batch_deposit;
use subtx_test::shard_run_config::{env_h160_optional, env_string, env_u128, ShardRunConfig};

#[subxt::subxt(
    runtime_metadata_path = "./deepx-node-metadata.scale",
    derive_for_all_types = "Eq, PartialEq, Clone, Debug"
)]
pub mod node_runtime {}

// ── 常量 ─────────────────────────────────────────────────────────────

const LENDING_MARKET_ID: u8 = 1;
const INIT_QUOTA: u32 = 429467295;
const PARALLEL_CHAIN_SCAN: usize = 64;
const RESUME_SKIP_ROOTER_QUOTA: bool = false;

static ROOTER: LazyLock<Keypair> = LazyLock::new(|| {
    let mut sk = [0u8; 32];
    sk.copy_from_slice(
        &hex::decode("349f7f21d09265b525c562df697cee56d65fe23fe638bb890dd2213a0cca5dcd").unwrap(),
    );
    Keypair::from_secret_key(sk).unwrap()
});

static PARALLEL_INIT: LazyLock<usize> = LazyLock::new(|| {
    env_u64("PARALLEL_SUBACCOUNT_INITS", 48).clamp(1, 96) as usize
});

static INIT_POLL_STEP_MS: LazyLock<u64> =
    LazyLock::new(|| env_u64("INIT_POLL_STEP_MS", 110).clamp(50, 2000));

static GLOBAL_API: Mutex<Option<OnlineClient<EthRuntimeConfig>>> = Mutex::const_new(None);

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

#[derive(Clone)]
struct AccountDetail {
    name: String,
    kp: Keypair,
    subaccount: H160,
    #[allow(dead_code)]
    order_num: u32,
}

fn evm_address(aid: &AccountId20) -> H160 {
    H160(aid.0)
}

fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

// ── RPC / API ───────────────────────────────────────────────────────

async fn get_api() -> anyhow::Result<OnlineClient<EthRuntimeConfig>> {
    if let Some(api) = GLOBAL_API.lock().await.as_ref() {
        return Ok(api.clone());
    }
    drop(GLOBAL_API.lock().await);
    let rpc = chain_ws::rpc_client().await.map_err(|e| anyhow::anyhow!("{e}"))?;
    let api = OnlineClient::<EthRuntimeConfig>::from_rpc_client(rpc)
        .await
        .map_err(|e| anyhow::anyhow!("OnlineClient: {e:?}"))?;
    *GLOBAL_API.lock().await = Some(api.clone());
    Ok(api)
}

async fn get_rpc() -> anyhow::Result<LegacyRpcMethods<EthRuntimeConfig>> {
    let rpc = chain_ws::rpc_client().await.map_err(|e| anyhow::anyhow!("{e}"))?;
    Ok(LegacyRpcMethods::new(rpc))
}

// ── 链上读 ───────────────────────────────────────────────────────────

async fn first_subaccount(user: &Keypair) -> Option<H160> {
    let api = get_api().await.ok()?;
    let q = node_runtime::storage()
        .subaccount()
        .user_stats_for(user.public_key().to_account_id().0.into());
    let stats = api.storage().at_latest().await.ok()?.fetch(&q).await.ok()??;
    stats.subaccounts.first().cloned()
}

async fn subaccount_order_num(sub: H160) -> anyhow::Result<u32> {
    let q = node_runtime::storage().subaccount().subaccount_info(sub);
    let info = get_api()
        .await?
        .storage()
        .at_latest()
        .await?
        .fetch(&q)
        .await?
        .ok_or_else(|| anyhow::anyhow!("subaccount_info missing for {sub:?}"))?;
    Ok(info.next_order_id.saturating_sub(1))
}

/// `frame_system::Account` 中 `quota == 0` 表示未激活（见 System 预编译文档）。
async fn system_account_quota(aid: &AccountId20) -> anyhow::Result<u32> {
    let q = node_runtime::storage().system().account(aid.clone());
    let info = get_api()
        .await?
        .storage()
        .at_latest()
        .await?
        .fetch(&q)
        .await?;
    Ok(info.map(|a| a.quota).unwrap_or(0))
}

async fn has_lending_deposit(sub: H160, asset: &[u8]) -> anyhow::Result<bool> {
    let q = node_runtime::storage().lending().positions_for(
        AccountId20 { 0: sub.0 },
        LENDING_MARKET_ID,
    );
    let positions = get_api().await?.storage().at_latest().await?.fetch(&q).await?;
    Ok(positions.is_some_and(|p| {
        p.deposits
            .iter()
            .any(|(k, amt)| k.0 == LENDING_MARKET_ID && k.1.0.as_slice() == asset && *amt > 0)
    }))
}

// ── ROOTER 提交（quota 等）────────────────────────────────────────────

async fn wait_rooter_nonce(root_id: &AccountId20, min_nonce: u64) -> anyhow::Result<()> {
    let api = get_api().await?;
    let timeout = env_u64("ROOTER_NONCE_WAIT_MS", 20_000);
    let step = env_u64("ROOTER_NONCE_POLL_MS", 50).max(10);
    let deadline = std::time::Instant::now() + Duration::from_millis(timeout);
    loop {
        if api.tx().account_nonce(root_id).await? >= min_nonce {
            return Ok(());
        }
        if std::time::Instant::now() >= deadline {
            anyhow::bail!(
                "ROOTER nonce 在 {timeout}ms 内未到 {min_nonce}；加大 ROOTER_NONCE_WAIT_MS 或检查出块"
            );
        }
        tokio::time::sleep(Duration::from_millis(step)).await;
    }
}

async fn submit_root(
    root_kp: &Keypair,
    call: &impl subxt::tx::Payload,
    root_nonce: &mut u64,
) -> anyhow::Result<()> {
    let api = get_api().await?;
    let rpc = get_rpc().await?;
    let root_id = root_kp.public_key().to_account_id();
    let nonce = *root_nonce;
    let signed = api
        .tx()
        .create_partial_offline(
            call,
            SubstrateExtrinsicParamsBuilder::new().nonce(nonce).build(),
        )?
        .sign(root_kp);
    if let Err(e) = rpc
        .author_submit_extrinsic(&Bytes::from_owner(signed.into_encoded()))
        .await
    {
        if root_tx_already_in_pool(&e) {
            let next = nonce + 1;
            wait_rooter_nonce(&root_id, next).await?;
            *root_nonce = api.tx().account_nonce(&root_id).await?;
            return Ok(());
        }
        return Err(anyhow::anyhow!("{e:?}"));
    }
    let next = nonce + 1;
    wait_rooter_nonce(&root_id, next).await?;
    *root_nonce = next;
    Ok(())
}

fn quota_already_activated(err: &impl std::fmt::Debug) -> bool {
    let s = format!("{err:?}").to_lowercase();
    s.contains("accountalreadyactivated") || s.contains("already activated")
}

fn root_tx_already_in_pool(err: &impl std::fmt::Debug) -> bool {
    let s = format!("{err:?}").to_lowercase();
    s.contains("priority is too low")
        || s.contains("1014")
        || s.contains("already imported")
        || s.contains("1013")
}

async fn ensure_quota(root_kp: &Keypair, aid: AccountId20, root_nonce: &mut u64) -> anyhow::Result<()> {
    let addr = evm_address(&aid);
    let mut quota = system_account_quota(&aid).await?;
    if quota >= INIT_QUOTA {
        return Ok(());
    }
    if quota == 0 {
        let activate = node_runtime::tx().quota().activate_account(aid.clone());
        match submit_root(root_kp, &activate, root_nonce).await {
            Ok(()) => {}
            Err(e) if quota_already_activated(&e) => {}
            Err(e) if root_tx_already_in_pool(&e) => {
                let root_id = root_kp.public_key().to_account_id();
                let pending = *root_nonce;
                wait_rooter_nonce(&root_id, pending + 1).await?;
                *root_nonce = get_api().await?.tx().account_nonce(&root_id).await?;
            }
            Err(e) => return Err(anyhow::anyhow!("activate_account {addr:?}: {e:#}")),
        }
        quota = system_account_quota(&aid).await?;
        if quota == 0 {
            anyhow::bail!("activate_account 后 quota 仍为 0: {addr:?}");
        }
    }
    if quota >= INIT_QUOTA {
        return Ok(());
    }
    let add = node_runtime::tx().quota().manager_add_quota(aid, INIT_QUOTA);
    submit_root(root_kp, &add, root_nonce).await
}

// ── 用户 initialize_subaccount ────────────────────────────────────────

async fn poll_until_subaccount(user: &Keypair, label: &str, timeout_ms: u64) -> anyhow::Result<H160> {
    let deadline = std::time::Instant::now() + Duration::from_millis(timeout_ms);
    loop {
        if let Some(h) = first_subaccount(user).await {
            return Ok(h);
        }
        if std::time::Instant::now() >= deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(*INIT_POLL_STEP_MS)).await;
    }
    Err(anyhow::anyhow!("轮询 {timeout_ms}ms 仍无子账户: {label}"))
}

async fn initialize_subaccount(user: &Keypair, label: &str) -> anyhow::Result<H160> {
    let addr = evm_address(&user.public_key().to_account_id());
    if let Some(h) = first_subaccount(user).await {
        return Ok(h);
    }
    let api = get_api().await?;
    let rpc = get_rpc().await?;
    let nonce = api.tx().account_nonce(&user.public_key().to_account_id()).await?;
    let call = node_runtime::tx().subaccount().initialize_subaccount(BoundedVec(
        label.as_bytes().to_vec(),
    ));
    let signed = api
        .tx()
        .create_partial_offline(
            &call,
            SubstrateExtrinsicParamsBuilder::new().nonce(nonce).build(),
        )?
        .sign(user);
    match rpc
        .author_submit_extrinsic(&Bytes::from_owner(signed.into_encoded()))
        .await
    {
        Ok(_) => {}
        Err(e) => warn!("initialize_subaccount submit {addr:?} ({label}): {e:?}"),
    }
    poll_until_subaccount(user, label, env_u64("INIT_POLL_MS", 8_000))
        .await
        .map_err(|_| anyhow::anyhow!("initialize 后仍无子账户: {addr:?} ({label})（检查 quota / RPC）"))
}

// ── 存款 ─────────────────────────────────────────────────────────────

async fn deposit_substrate(
    root_kp: &Keypair,
    sub: H160,
    asset: &str,
    amount: u128,
    root_nonce: &mut u64,
) -> anyhow::Result<()> {
    let api = get_api().await?;
    let rpc = get_rpc().await?;
    let call = node_runtime::tx().lending().deposit(
        None,
        sub,
        LENDING_MARKET_ID,
        BoundedVec(asset.as_bytes().to_vec()),
        amount,
    );
    let signed = api
        .tx()
        .create_partial_offline(
            &call,
            SubstrateExtrinsicParamsBuilder::new().nonce(*root_nonce).build(),
        )?
        .sign(root_kp);
    match rpc
        .author_submit_extrinsic(&Bytes::from_owner(signed.into_encoded()))
        .await
    {
        Ok(_) => *root_nonce = root_nonce.saturating_add(2),
        Err(e) => {
            warn!("deposit {sub:?}: {e:?}");
            *root_nonce = api
                .tx()
                .account_nonce(&root_kp.public_key().to_account_id())
                .await
                .unwrap_or(*root_nonce);
        }
    }
    Ok(())
}

async fn deposit_batch_multicall(
    root_kp: &Keypair,
    multicall: H160,
    subs: &[H160],
    asset: &str,
    amount: u128,
    evm_nonce: &mut u64,
) -> anyhow::Result<EvmReceipt> {
    anyhow::ensure!(!subs.is_empty(), "empty multicall batch");
    let api = get_api().await?;
    let rpc = chain_ws::rpc_client().await.map_err(|e| anyhow::anyhow!("{e}"))?;
    let chain_id = api
        .storage()
        .at_latest()
        .await?
        .fetch(&node_runtime::storage().evm_chain_id().chain_id())
        .await?
        .ok_or_else(|| anyhow::anyhow!("无法读取 evm_chain_id"))?;

    let root_h160: H160 = root_kp.public_key().to_account_id().0.into();
    let on_chain = evm_rpc::eth_transaction_count(&rpc, root_h160).await?;
    if on_chain > *evm_nonce {
        warn!("EVM nonce 对齐 {} -> {on_chain}", *evm_nonce);
        *evm_nonce = on_chain;
    }

    let gas_limit = 200_000u64
        + env_u64("ROOTER_MULTICALL_GAS_PER_ACCOUNT", 180_000) * subs.len() as u64;
    let eip1559 = EthEip1559 {
        chain_id,
        nonce: (*evm_nonce).into(),
        max_priority_fee_per_gas: env_u64("ROOTER_EVM_MAX_PRIORITY_FEE_PER_GAS", 0).into(),
        max_fee_per_gas: env_u64("ROOTER_EVM_MAX_FEE_PER_GAS", 0).into(),
        gas_limit: gas_limit.into(),
        action: EthTxAction::Call(multicall.0.into()),
        value: 0u64.into(),
        input: encode_batch_deposit(subs, asset.as_bytes(), amount),
        access_list: vec![],
        odd_y_parity: false,
        r: Default::default(),
        s: Default::default(),
    };
    let (raw, _) = evm_rpc::build_signed_raw_eip1559(eip1559, root_kp)?;
    let tx_hash = evm_rpc::eth_send_raw_transaction(&rpc, &raw).await?;
    let receipt = evm_rpc::wait_transaction_receipt(&rpc, tx_hash).await?;
    if receipt.success {
        *evm_nonce += 1;
        Ok(receipt)
    } else {
        anyhow::bail!("Multicall revert {tx_hash:?} block={:?}", receipt.block_number)
    }
}

async fn wait_batch_deposits(subs: &[H160], asset: &[u8]) -> anyhow::Result<Vec<H160>> {
    let timeout = env_u64("ROOTER_MULTICALL_SETTLE_MS", 8_000);
    let step = env_u64("ROOTER_MULTICALL_POLL_MS", 400).max(100);
    let deadline = std::time::Instant::now() + Duration::from_millis(timeout);
    loop {
        let missing: Vec<_> = {
            let mut m = Vec::new();
            for &s in subs {
                if !has_lending_deposit(s, asset).await? {
                    m.push(s);
                }
            }
            m
        };
        if missing.is_empty() {
            return Ok(vec![]);
        }
        if std::time::Instant::now() >= deadline {
            return Ok(missing);
        }
        tokio::time::sleep(Duration::from_millis(step)).await;
    }
}

async fn run_multicall_deposits(
    root_kp: &Keypair,
    multicall: H160,
    accounts: &[AccountDetail],
    need: &[usize],
    asset: &str,
    amount: u128,
    batch_size: usize,
    root_nonce: &mut u64,
) -> anyhow::Result<u32> {
    ensure_quota(root_kp, AccountId20 { 0: multicall.0 }, root_nonce).await?;

    let rpc = chain_ws::rpc_client().await.map_err(|e| anyhow::anyhow!("{e}"))?;
    let root_h160: H160 = root_kp.public_key().to_account_id().0.into();
    let mut evm_nonce = evm_rpc::eth_transaction_count(&rpc, root_h160).await?;
    let evm_start = evm_nonce;

    let mut pending: Vec<usize> = need.to_vec();
    let mut batch_size = batch_size.clamp(1, 50);
    let mut confirmed = 0u32;
    let mut evm_txs = 0u32;
    let asset_b = asset.as_bytes();

    info!(
        "Multicall: {} 户 {:?} batch≤{} contract={multicall:?} evm_nonce={evm_start}",
        need.len(),
        asset,
        batch_size
    );

    while !pending.is_empty() {
        let n_chunks = pending.len().div_ceil(batch_size);
        let mut still = Vec::new();
        for (chunk_i, chunk) in pending.chunks(batch_size).enumerate() {
            let subs: Vec<H160> = chunk.iter().map(|&i| accounts[i].subaccount).collect();
            if deposit_batch_multicall(root_kp, multicall, &subs, asset, amount, &mut evm_nonce)
                .await
                .is_err()
            {
                warn!("multicall EVM {}/{} 失败", chunk_i + 1, n_chunks);
                still.extend(chunk.iter().copied());
                evm_txs += 1;
                continue;
            }
            evm_txs += 1;
            let missing = wait_batch_deposits(&subs, asset_b).await?;
            confirmed += (chunk.len() - missing.len()) as u32;
            for &i in chunk {
                if missing.contains(&accounts[i].subaccount) {
                    still.push(i);
                }
            }
        }
        if still.len() == pending.len() && batch_size > 1 {
            batch_size = (batch_size / 2).max(1);
            warn!("Multicall 无进展，batch_size -> {batch_size}");
            continue;
        }
        if still.len() == pending.len() && batch_size == 1 {
            anyhow::bail!(
                "Multicall 失败 {} 户；确认合约 {multicall:?} 有足够 USDC 且 ROOTER 为 owner",
                still.len()
            );
        }
        pending = still;
    }

    info!(
        "Multicall 完成 {}/{} 户, {evm_txs} 笔 EVM (nonce {evm_start}..={})",
        confirmed,
        need.len(),
        evm_nonce.saturating_sub(1)
    );
    Ok(confirmed)
}

// ── 账户准备（扫描 / quota / initialize）──────────────────────────────

fn create_test_accounts(first: u32, count: u32) -> anyhow::Result<Vec<AccountDetail>> {
    (0..count)
        .map(|i| {
            let addr_idx = first + i;
            let kp = Keypair::from_phrase(
                &bip39::Mnemonic::from_str(DEV_PHRASE)?,
                None,
                DerivationPath::eth(0, addr_idx),
            )?;
            Ok(AccountDetail {
                name: format!("test_user_{addr_idx}"),
                kp,
                subaccount: Default::default(),
                order_num: 0,
            })
        })
        .collect()
}

/// 并发扫描：返回每个账户是否已有子账户地址。
async fn scan_existing_subaccounts(accounts: &[AccountDetail]) -> Vec<Option<H160>> {
    let n = accounts.len();
    let mut out = vec![None; n];
    for start in (0..n).step_by(PARALLEL_CHAIN_SCAN) {
        let end = (start + PARALLEL_CHAIN_SCAN).min(n);
        let mut set = JoinSet::new();
        for i in start..end {
            let kp = accounts[i].kp.clone();
            set.spawn(async move {
                let sub = first_subaccount(&kp).await;
                Ok::<_, anyhow::Error>((i, sub))
            });
        }
        while let Some(r) = set.join_next().await {
            let (i, sub) = r.unwrap().unwrap();
            out[i] = sub;
        }
    }
    out
}

async fn fill_subaccount_info(accounts: &mut [AccountDetail], sub: H160, idx: usize) -> anyhow::Result<()> {
    accounts[idx].subaccount = sub;
    accounts[idx].order_num = subaccount_order_num(sub).await?;
    Ok(())
}

async fn quota_and_initialize(
    accounts: &mut [AccountDetail],
    need_init: &[(usize, Keypair, String)],
    root_kp: &Keypair,
    root_nonce: &mut u64,
) -> anyhow::Result<()> {
    if need_init.is_empty() {
        return Ok(());
    }
    info!("quota: {} 户", need_init.len());
    if !RESUME_SKIP_ROOTER_QUOTA {
        let total = need_init.len();
        for (n, (_, kp, _)) in need_init.iter().enumerate() {
            ensure_quota(root_kp, kp.public_key().to_account_id(), root_nonce).await?;
            if (n + 1) % 10 == 0 || n + 1 == total {
                info!("quota 进度 {}/{}", n + 1, total);
            }
        }
    }

    let settle = env_u64("ROOTER_POST_QUOTA_BATCH_SETTLE_MS", 1500);
    tokio::time::sleep(Duration::from_millis(settle)).await;

    let par = *PARALLEL_INIT;
    info!("initialize: {} 户, 并行 {par}", need_init.len());
    for chunk in need_init.chunks(par) {
        let mut set = JoinSet::new();
        for (idx, kp, name) in chunk {
            let kp = kp.clone();
            let name = name.clone();
            let idx = *idx;
            set.spawn(async move {
                let sub = initialize_subaccount(&kp, &name).await?;
                Ok::<_, anyhow::Error>((idx, sub))
            });
        }
        while let Some(r) = set.join_next().await {
            let (idx, sub) = r.map_err(|e| anyhow::anyhow!("initialize join: {e}"))??;
            fill_subaccount_info(accounts, sub, idx).await?;
        }
    }
    Ok(())
}

async fn indices_needing_deposit(accounts: &[AccountDetail], asset: &[u8]) -> anyhow::Result<Vec<usize>> {
    let n = accounts.len();
    let mut need = Vec::new();
    for start in (0..n).step_by(PARALLEL_CHAIN_SCAN) {
        let end = (start + PARALLEL_CHAIN_SCAN).min(n);
        let mut set = JoinSet::new();
        for i in start..end {
            let acc = accounts[i].clone();
            let asset = asset.to_vec();
            set.spawn(async move {
                let skip = has_lending_deposit(acc.subaccount, &asset).await?;
                Ok::<_, anyhow::Error>((i, skip))
            });
        }
        while let Some(r) = set.join_next().await {
            let (i, skip) = r.map_err(|e| anyhow::anyhow!("deposit 检查 join: {e}"))??;
            if !skip {
                need.push(i);
            }
        }
    }
    need.sort_unstable();
    Ok(need)
}

// ── main ─────────────────────────────────────────────────────────────

#[tokio::main(flavor = "multi_thread", worker_threads = 8)]
async fn main() -> anyhow::Result<()> {
    env_logger::init();
    let cfg = ShardRunConfig::load()?;
    let asset = env_string("ROOTER_DEPOSIT_ASSET", "usdc");
    anyhow::ensure!(!asset.is_empty(), "ROOTER_DEPOSIT_ASSET 不能为空");
    let amount = env_u128("ROOTER_DEPOSIT_AMOUNT", 100_000_000);
    anyhow::ensure!(amount > 0, "ROOTER_DEPOSIT_AMOUNT 必须 > 0");
    let multicall = env_h160_optional("ROOT_DEPOSIT_MULTICALL")?;
    let mc_batch = env_u64("DEPOSIT_MULTICALL_BATCH_SIZE", 25).clamp(1, 100) as usize;

    info!("rooter_deposit {} | {asset:?} amount={amount}", cfg.summary());
    if let Some(mc) = multicall {
        info!("deposit: Multicall {mc:?} batch={mc_batch}");
    }

    chain_ws::init(cfg.ws_url.clone()).map_err(|e| anyhow::anyhow!("{e}"))?;
    get_api().await?;
    tokio::time::sleep(Duration::from_millis(500)).await;

    let mut accounts = create_test_accounts(cfg.first_addr_index, cfg.account_count)?;
    let root_kp = ROOTER.clone();
    let mut root_nonce = get_api()
        .await?
        .tx()
        .account_nonce(&root_kp.public_key().to_account_id())
        .await?;

    // 1. 扫描并划分需 initialize 的账户
    info!("扫描子账户 ({} 户)…", accounts.len());
    let existing = scan_existing_subaccounts(&accounts).await;
    let mut need_init = Vec::new();
    for (i, sub) in existing.into_iter().enumerate() {
        if let Some(s) = sub {
            fill_subaccount_info(&mut accounts, s, i).await?;
        } else {
            need_init.push((i, accounts[i].kp.clone(), accounts[i].name.clone()));
        }
    }
    info!(
        "子账户: 已有 {} / 需新建 {}",
        accounts.len() - need_init.len(),
        need_init.len()
    );

    // 2. quota + initialize
    quota_and_initialize(&mut accounts, &need_init, &root_kp, &mut root_nonce).await?;

    // 3. 存款
    let need_dep = indices_needing_deposit(&accounts, asset.as_bytes()).await?;
    info!(
        "deposit {asset:?}: 待充 {} / 已有 {}",
        need_dep.len(),
        accounts.len() - need_dep.len()
    );

    let done = if let Some(mc) = multicall {
        run_multicall_deposits(
            &root_kp,
            mc,
            &accounts,
            &need_dep,
            &asset,
            amount,
            mc_batch,
            &mut root_nonce,
        )
        .await?
    } else {
        for &i in &need_dep {
            deposit_substrate(
                &root_kp,
                accounts[i].subaccount,
                &asset,
                amount,
                &mut root_nonce,
            )
            .await?;
        }
        need_dep.len() as u32
    };

    let skipped = (accounts.len() - need_dep.len()) as u32;
    info!(
        "完成: 新充 {done} 笔, 跳过 {skipped} 笔, 共 {} 户",
        accounts.len()
    );
    Ok(())
}
