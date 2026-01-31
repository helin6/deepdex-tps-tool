#![allow(missing_docs)]
#![allow(dead_code)]

use crate::node_runtime::perp_market::calls::types;
use crate::node_runtime::perp_market::calls::types::place_order::OrderType;
use crate::node_runtime::runtime_types::ethereum::transaction::{EIP1559Transaction, TransactionAction, TransactionV2};
use crate::node_runtime::runtime_types::pallet_primitives::types::{MarketSpec, PostOnlyParam};
use crate::node_runtime::runtime_types::primitive_types::U256;
use crate::node_runtime::runtime_types::sp_arithmetic::fixed_point::FixedI128;
use bytes::Bytes;
use node_runtime::runtime_types::bounded_collections::bounded_vec::BoundedVec;
use node_runtime::runtime_types::sp_arithmetic::fixed_point::FixedU128;
use std::str::FromStr;
use std::sync::LazyLock;
use std::time::Duration;
use subxt::backend::rpc::RpcClient;
use subxt::config::substrate::SubstrateExtrinsicParamsBuilder;
use subxt::config::{SubstrateExtrinsicParams, substrate};
use subxt::ext::subxt_core::utils::AccountId20;
use subxt::ext::subxt_rpcs::LegacyRpcMethods;
use subxt::utils::H160;
use subxt::{Config, OnlineClient};
use subxt_signer::eth::Signature;
use subxt_signer::eth::dev;
use subxt_signer::eth::{DerivationPath, Keypair};
use tokio::time::Instant;
use tokio::{self};

#[subxt::subxt(
    runtime_metadata_path = "./polkadot_metadata_dev.scale",
    derive_for_all_types = "Eq, PartialEq, Clone, Debug"
)]
pub mod node_runtime {}

use crate::node_runtime::system::events::remarked::Hash;
use log::{debug, error, info, warn};
use precompile_utils::solidity::codec::Writer as EvmDataWriter;
use secp256k1::ecdsa::RecoveryId;
use subxt::ext::codec::{Compact, Encode};
use subxt::ext::futures::future::join_all;
use subxt_signer::{DEV_PHRASE, bip39};
use crate::node_runtime::perp_market::calls::types::cancel_order::CancelReason;

// 0x18ae37ea
const PERP_PLACE_ORDER_SELECTOR: [u8; 4] = [24, 174, 55, 234];

const NODE_WS_ADDR: &str = "ws://127.0.0.1:9944";

static ROOTER: LazyLock<Keypair> = LazyLock::new(|| dev::alith());

const MAX_ACTIVE_ORDERS: u32 = 5000;

const SIZE_OF_EACH_ORDER: u128 = 10_000;

const MATCHED_PERCENT: u32 = 0; // 1%

const PENDING_NUM: u32 = 1;

#[tokio::main(flavor = "multi_thread", worker_threads = 10)]
async fn main() -> anyhow::Result<()> {
    env_logger::init();
    let market_num: u32 = 5;

    // 准备订单簿，市场等环境
    let market_ids = prepare_env(market_num).await?;
    let init_quota = 429467295;

    // 创建额外账户
    // const N: usize = 10;
    const N: usize = 16;

    let mut test_accounts = create_extra_test_accounts(N as u32 * 5 * 5 + 2).await?; //'2' means extra account to place pending orders

    tokio::time::sleep(Duration::from_millis(5000)).await;

    info!("create subaccounts......");

    // 创建子账户
    for x in test_accounts.iter() {
        let x = x.clone();
        let _ = get_first_subaccount_ensure_exist(&x.kp, x.name.as_str()).await;
    }


    tokio::time::sleep(Duration::from_millis(5000)).await;

    for x in test_accounts.iter_mut() {
        if let Ok(subaccounts) = get_subaccount(&x.kp).await {
            if let Some(subaccount) = subaccounts.get(0) {
                info!("find subaccount: {:?}", subaccount);
                let account_detail = AccountDetail { name: x.name.clone(), kp: x.kp.clone(), subaccount: subaccount.clone() };
                *x = account_detail;
            } else {
                warn!("subaccount not find for {:?}", x.name);
            }
        } else {
            panic!("get_subaccount return error")
        }
    }

    // 子账户存款
    let amount = 100_000_000_000_000_000_000_000_000_000u128;

    // let mut tasks = Vec::new();
    for x in &test_accounts {
        let kp = x.kp.clone();
        let subaccount = x.subaccount.clone();
        // let t = tokio::spawn(async move {
            info!("do deposit by account: {:?}", hex::encode(&kp.public_key().to_account_id().0));
            deposit(&kp, &subaccount, 1, "USDT", amount).await.unwrap();
        // });
        // tasks.push(t);
    }
    // join_all(tasks).await;
    tokio::time::sleep(Duration::from_millis(5000)).await;


    // let market_id_btc_usdt = get_market_id_by_name("BTC_USDT").await?;
    // let market_id_eth_usdt = get_market_id_by_name("ETH_USDT").await?;
    // let market_id_sol_usdt = get_market_id_by_name("SOL_USDT").await?;
    // let market_id_trx_usdt = get_market_id_by_name("TRX_USDT").await?;
    // let market_id_doge_usdt = get_market_id_by_name("DOGE_USDT").await?;

    // (name, keypair, subaccount, market_id, is_long, price, order_type)
    let mut place_order_stubs = Vec::new();


    for (index, id) in market_ids.iter().enumerate() {
        let test_accounts: &[AccountDetail] = &test_accounts[5 * index * N.. 5 * (index + 1) * N];
        push_order_pairs_for_market(&mut place_order_stubs, test_accounts, *id, 50_000_000, index)?;

    }
    // let test_accounts_for_btc: &[AccountDetail] = &test_accounts[0.. 5 * N];
    // let test_accounts_for_eth: &[AccountDetail] = &test_accounts[5 * N..10 * N];
    // let test_accounts_for_sol: &[AccountDetail] = &test_accounts[10 * N..15 * N];
    // let test_accounts_for_trx: &[AccountDetail] = &test_accounts[15 * N..20 * N];
    // let test_accounts_for_doge: &[AccountDetail] = &test_accounts[20 * N..25 * N];


    // push_order_pairs_for_market(&mut place_order_stubs, test_accounts_for_btc, market_id_btc_usdt, 50_000_000, "btc")?;
    // push_order_pairs_for_market(&mut place_order_stubs, test_accounts_for_eth, market_id_eth_usdt, 3_000_000, "eth")?;
    // push_order_pairs_for_market(&mut place_order_stubs, test_accounts_for_sol, market_id_sol_usdt, 100_000, "sol")?;
    // push_order_pairs_for_market(&mut place_order_stubs, test_accounts_for_trx, market_id_trx_usdt, 100_000, "trx")?;
    // push_order_pairs_for_market(&mut place_order_stubs, test_accounts_for_doge, market_id_doge_usdt, 100_000, "doge")?;

    let place_order_func = place_order_no_wait_response_old_batch;
    let mut tasks = Vec::new();

    // 目前测试，单市场的流量限制为6000 tx/s，能保证完全撮合，超过该值会有很多订单状态不正常
    // let rate = 120; // single thread(full-matched), tps: 6000
    // let rate = 250; // single thread(non-matched), tps: 37500(250*150(acc))
    // let rate = 70; // multi thread(5, full-matched), tps: 17500
    let rate = 240; // multi thread(5, non-matched), tps: 100000(250 * 80(acc) * 5)
    // let rate = 200;

    let n = rate * 60;
    // let n = rate * 1;

    for (name, keypair, subaccount, market_id, is_long, price, match_price, order_type) in place_order_stubs.clone() {
        // clone for move
        let name = name.to_string();
        let keypair = keypair.clone();

        let task = tokio::spawn(async move {
            let res = place_order_func(&name, &keypair, subaccount, market_id, is_long, price, match_price, order_type,  n, rate).await;

            match res {
                Ok(_) => info!("{name} orders placed successfully"),
                Err(e) => error!("Error placing order for {name}: {:?}", e),
            }
        });

        tasks.push(task);

        tokio::time::sleep(Duration::from_millis(50)).await; // 避免启动的时候一次性发送大量请求
    }

    join_all(tasks).await;

    tokio::time::sleep(Duration::from_secs(5)).await;

    // 最后查询一下所有的仓位和挂单
    let api = get_api().await?;
    for (i, (name, keypair, subaccount, market_id, is_long, price, match_price, order_type)) in place_order_stubs.iter().enumerate() {
        let subaccount_info_query = node_runtime::storage().subaccount().subaccount_info(subaccount.clone());
        let subaccount_info = api.storage().at_latest().await?.fetch(&subaccount_info_query).await?;
        let total_order = if let Some(info) = subaccount_info {
            // info!("account: {:?}, name: {}, total orders: {}", subaccount, name, info.next_order_id.saturating_sub(1));
            info.next_order_id.saturating_sub(1)
        } else {
            0
        };

        let position_query = node_runtime::storage().perp_market().user_perp_positions(subaccount.clone());
        let positions = api.storage().at_latest().await?.fetch(&position_query).await?;
        if let Some(positions) = positions {
            for p in positions {
                let orders_query = node_runtime::storage()
                    .perp_market()
                    .active_perp_orders_for(subaccount.clone(), p.market_id);
                let orders = api.storage().at_latest().await?.fetch(&orders_query).await?;
                let pending_orders_count = if let Some(orders) = orders { orders.len() } else { 0 };

                let matched_orders_count = p.base_asset_amount / SIZE_OF_EACH_ORDER;
                info!(
                    "[{i}]{:?}  subaccount: {:?}, market_id: {}, is_long {}, total_orders {}, matched_orders {}, pending_orders {}, unknown_orders {}",
                    name,
                    subaccount,
                    p.market_id,
                    p.is_long,
                    total_order,
                    matched_orders_count,
                    pending_orders_count,
                    n / 2 - matched_orders_count as u32 - pending_orders_count as u32
                );
            }
        } else {
            warn!("[{i}]{:?}, no positions found", name);
            let orders_query = node_runtime::storage()
                .perp_market()
                .active_perp_orders_for(subaccount.clone(), *market_id);
            let orders = api.storage().at_latest().await?.fetch(&orders_query).await?;
            let pending_orders_count = if let Some(orders) = orders { orders.len() } else { 0 };
            info!(
                "[{i}]{}, subaccount: {:?}, market_id: {}, is_long {}, total_orders {}, matched_orders {}, pending_orders {}, cancelled_orders {}",
                name,
                subaccount,
                market_id,
                is_long,
                total_order,
                0,
                pending_orders_count,
                total_order as usize - pending_orders_count
            );
        }
    }

    Ok(())
}

fn push_order_pairs_for_market(
    stubs: &mut Vec<PlaceOrderItem>,
    test_accounts: &[AccountDetail],
    market_id: u16,
    price: u128,
    i: usize,
) -> anyhow::Result<()> {
    let mut price_diff = 1;
    for chunk in test_accounts.chunks(2) {
        if let [account_1, account_2] = chunk {
            let (t1, t2) = build_place_order_pair(market_id, price, account_1, account_2, price_diff);
            stubs.push(t1);
            stubs.push(t2);
            price_diff += 1;
        } else {
            return Err(anyhow::anyhow!("place_order_stubs market:{} test_accounts failed", i));
        }
    }
    Ok(())
}

type PlaceOrderItem = (String, Keypair, H160, u16, bool, u128, u128, OrderType);
fn build_place_order_pair(market_id: u16, price: u128, account_1: &AccountDetail, account_2: &AccountDetail, price_diff: u128) -> (PlaceOrderItem, PlaceOrderItem) {
    (
        (
            account_1.name.clone(),
            account_1.kp.clone(),
            account_1.subaccount,
            market_id,
            true,
            price - price_diff,
            price,
            OrderType::Limit,
        ),
        (
            account_2.name.clone(),
            account_2.kp.clone(),
            account_2.subaccount,
            market_id,
            false,
            price + price_diff,
            price,
            OrderType::Limit,
        ),
    )
}

async fn get_api() -> anyhow::Result<OnlineClient<EthRuntimeConfig>> {
    let api = OnlineClient::<EthRuntimeConfig>::from_url("ws://localhost:9944").await?;
    Ok(api)
}

async fn place_order_no_wait_response_evm(
    user_name: &str,
    user: &Keypair,
    subaccount: H160,
    market_id: u16,
    is_long: bool,
    price: u128,
    match_price: u128,
    order_type: OrderType,
    n: u32,
    rate: u32,
) -> anyhow::Result<()> {
    let api = get_api().await?;

    let chain_id_query = node_runtime::storage().evm_chain_id().chain_id();

    let chain_id = api
        .storage()
        .at_latest()
        .await?
        .fetch(&chain_id_query)
        .await?
        .expect("fail to get chain_id");

    let order_type_u8 = match order_type {
        OrderType::Limit => 0u8,
        OrderType::Market => 1,
        OrderType::Stop => 2,
    };
    let input = EvmDataWriter::new_with_selector(u32::from_be_bytes(PERP_PLACE_ORDER_SELECTOR))
        .write(precompile_utils::prelude::Address(subaccount.0.into()))
        .write(market_id)
        .write(is_long)
        .write(SIZE_OF_EACH_ORDER) // size
        .write(price)
        .write(order_type_u8)
        .write(2) // leverage
        .write(0) // take_profit
        .write(0) // stop_loss
        .write(false) // reduce_only
        .write(0) // post_only
        .build();

    // let mut nonce = api.tx().account_nonce(&user.public_key().to_account_id()).await?;
    let mut nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("Time went backwards")
        .as_millis() as u64;


    const PERP_ADDRESS: [u8; 20] = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4, 78];

    let mut calls = Vec::new();
    for _i in 1..=n {
        let epi1559_tx = ethereum::EIP1559Transaction {
            chain_id,
            nonce: nonce.into(),
            max_priority_fee_per_gas: 1_500_000_000u64.into(),
            max_fee_per_gas: 4_500_000_000u64.into(),
            gas_limit: 500_000u64.into(),
            action: ethereum::TransactionAction::Call(PERP_ADDRESS.into()),
            value: 0.into(),
            input: input.clone(),
            access_list: vec![],
            odd_y_parity: false,
            r: Default::default(),
            s: Default::default(),
        };

        let transaction = build_epi1559_tx_to_v2(epi1559_tx, user)?;
        let source_acc = user.public_key().to_account_id().0.into();
        let call = node_runtime::tx()
            .ethereum()
            .transact(transaction, source_acc);

        // let params = SubstrateExtrinsicParamsBuilder::new().nonce(nonce).build();
        let tx = api.tx().create_unsigned(&call)?;
        let encoded_tx = Bytes::from_owner(tx.into_encoded());

        calls.push((encoded_tx, source_acc, nonce));
        nonce = nonce + 1;
    }

    let rpc_client = RpcClient::from_url(NODE_WS_ADDR).await?;
    let rpc = LegacyRpcMethods::<EthRuntimeConfig>::new(rpc_client);

    let mut start = Instant::now();
    let interval = Duration::from_micros(1_000_000 / rate as u64);
    let mut next_tick = Instant::now();

    for (i, (call, source_acc, nonce)) in calls.iter().enumerate() {
        // 等到下一次发送时刻
        tokio::time::sleep_until(next_tick.into()).await;
        next_tick += interval;

        match rpc.author_submit_extrinsic(call).await {
            Ok(hash) => {
                debug!("{source_acc:?} submitted tx with nonce: {nonce}, hash: {hash:?}")
            },
            Err(e) => {
                warn!("Error submitting extrinsic for {user_name} at order {i}: {:?}", e);
                tokio::time::sleep(Duration::from_millis(50)).await;
            },
        }

        if (i + 1) % (rate as usize) == 0 {
            let elapsed = start.elapsed().as_millis();
            info!("{user_name}:{subaccount:?} submitted {rate} orders, cost {} ms", elapsed);
            start = Instant::now();
        }
    }

    Ok(())
}

async fn place_order_no_wait_response_old_batch(
    user_name: &str,
    user: &Keypair,
    subaccount: H160,
    market_id: u16,
    is_long: bool,
    price: u128,
    match_price: u128,
    order_type: OrderType,
    mut n: u32,
    rate: u32,
) -> anyhow::Result<()> {
    let api = get_api().await?;
    let mut nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("Time went backwards")
        .as_millis() as u64;

    let rpc_client = RpcClient::from_url(NODE_WS_ADDR).await?;
    let rpc = LegacyRpcMethods::<EthRuntimeConfig>::new(rpc_client);

    let mut start = Instant::now();
    let interval_inner = 1000 / rate as u64;
    let interval = Duration::from_millis(interval_inner.saturating_mul(8) / 10); // 80&
    let mut next_tick = Instant::now();
    let mut cancel_id: u32 = 0;
    // let mut total_encode_inner = Vec::new();
    let mut encoded_inner = Vec::new();
    let chunk_size = rate;

    let mut inner_num: u32 = 0;
    let mut skip_cancel = false;
    if user_name.starts_with("extra_pending_user") {
        n = PENDING_NUM * 2;
    }

    for i in 1..=n {
        let signed_tx_bytes = if i % 2 == 1 {
            debug!("{user_name} build place order: {} tx for i: {i}", cancel_id + 1);
            cancel_id += 1;
            let price = target_price(
                user_name,
                is_long,
                price,
                match_price,
                cancel_id,
                &mut skip_cancel,
            );
            let call = node_runtime::tx().perp_market().place_order(
                subaccount,
                market_id,
                is_long,
                10_000,
                price,
                order_type.clone(),
                None,
                2,
                None,
                None,
                false,
                PostOnlyParam::None,
            );
            // 等到下一次发送时刻
            tokio::time::sleep_until(next_tick.into()).await;
            next_tick += interval;

            let params = SubstrateExtrinsicParamsBuilder::new().nonce(nonce).build();
            let signed_tx = api.tx().create_partial_offline(&call, params)?.sign(user);
            Bytes::from_owner(signed_tx.into_encoded())
        } else {
            if user_name.starts_with("extra_pending_user") {
                continue;
            }
            if skip_cancel {
                skip_cancel = false;
                continue;
            }
            let order_id = i.saturating_sub(cancel_id);
            debug!("{user_name} build cancel order: {order_id} tx for i: {i}");
            let call = node_runtime::tx().perp_market().cancel_order(
                subaccount,
                order_id,
                market_id,
                CancelReason::UserCanceled,
            );
            // 等到下一次发送时刻
            tokio::time::sleep_until(next_tick.into()).await;
            next_tick += interval;

            let params = SubstrateExtrinsicParamsBuilder::new().nonce(nonce).build();
            let signed_tx = api.tx().create_partial_offline(&call, params)?.sign(user);
            Bytes::from_owner(signed_tx.into_encoded())
        };
        encoded_inner.extend(signed_tx_bytes);
        inner_num += 1;
        if inner_num >= chunk_size {
            loop {
                let mut extrinsics = Vec::new();
                Compact(inner_num).encode_to(&mut extrinsics);
                extrinsics.extend(encoded_inner.clone());
                match rpc.author_submit_extrinsics(&extrinsics).await {
                    Ok(batch_res) => {
                        for res in batch_res {
                            if let Err(e) = res {
                                warn!("Error submitting inner extrinsics for {user_name}: {:?}, try again", e);
                                // tokio::time::sleep(Duration::from_millis(600)).await;
                                // continue;
                            }
                        }
                        info!("{user_name} Submitting batch extrinsics successfully");
                        break;
                    }
                    Err(e) => {
                        warn!("Error submitting batch extrinsics for {user_name}: {:?}, try again", e);
                        tokio::time::sleep(Duration::from_millis(600)).await;
                        continue;
                    }
                }
            }
            inner_num = 0;
            encoded_inner = Vec::new();
            tokio::time::sleep_until(next_tick.into()).await;
            next_tick += interval * chunk_size;
        }
        nonce += 1;
    }

    // let mut next_tick = Instant::now();
    //
    // for (index, tx_chunk) in total_encode_inner.iter().enumerate() {
    //     info!("tx_chunk: {:?}", hex::encode(tx_chunk));
    //     match rpc.author_submit_extrinsics(&tx_chunk).await {
    //         Ok(_) => {
    //             info!("{user_name} Submitting batch extrinsics for index: {index} successfully");
    //             break;
    //         }
    //         Err(e) => {
    //             warn!("Error submitting batch extrinsics for {user_name}: {:?}", e);
    //             continue;
    //         }
    //     }
    //     tokio::time::sleep_until(next_tick.into()).await;
    //     next_tick += interval * chunk_size;
    // }
    Ok(())
}

async fn place_order_no_wait_response_old(
    user_name: &str,
    user: &Keypair,
    subaccount: H160,
    market_id: u16,
    is_long: bool,
    price: u128,
    match_price: u128,
    order_type: OrderType,
    n: u32,
    rate: u32,
) -> anyhow::Result<()> {
    let api = get_api().await?;
    let mut nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("Time went backwards")
        .as_millis() as u64;

    let rpc_client = RpcClient::from_url(NODE_WS_ADDR).await?;
    let rpc = LegacyRpcMethods::<EthRuntimeConfig>::new(rpc_client);

    let mut start = Instant::now();
    let interval_inner = 1000 / rate as u64;
    let interval = Duration::from_millis(interval_inner.saturating_mul(8) / 10); // 80&
    let mut next_tick = Instant::now();
    let mut cancel_id: u32 = 0;
    for i in 1..=n {
        let signed_tx_bytes = if i % 2 == 1 {
            info!("{user_name} submitted place order: {} tx for i: {i}", cancel_id + 1);
            cancel_id += 1;
            let call = node_runtime::tx().perp_market().place_order(
                subaccount,
                market_id,
                is_long,
                10_000,
                price,
                order_type.clone(),
                None,
                2,
                None,
                None,
                false,
                PostOnlyParam::None,
            );
            // 等到下一次发送时刻
            tokio::time::sleep_until(next_tick.into()).await;
            next_tick += interval;

            let params = SubstrateExtrinsicParamsBuilder::new().nonce(nonce).build();
            let signed_tx = api.tx().create_partial_offline(&call, params)?.sign(user);
            Bytes::from_owner(signed_tx.into_encoded())
        } else {
            let order_id = i.saturating_sub(cancel_id);
            info!("{user_name} submitted cancel order: {order_id} tx for i: {i}");
            let call = node_runtime::tx().perp_market().cancel_order(
                subaccount,
                order_id,
                market_id,
                CancelReason::UserCanceled,
            );
            // 等到下一次发送时刻
            tokio::time::sleep_until(next_tick.into()).await;
            next_tick += interval;

            let params = SubstrateExtrinsicParamsBuilder::new().nonce(nonce).build();
            let signed_tx = api.tx().create_partial_offline(&call, params)?.sign(user);
            Bytes::from_owner(signed_tx.into_encoded())
        };

        // 等到下一次发送时刻
        tokio::time::sleep_until(next_tick.into()).await;
        next_tick += interval;
        match rpc.author_submit_extrinsic(&signed_tx_bytes).await {
            Ok(_) => {
                nonce += 1;
                // i += 1;
            }
            Err(e) => {
                warn!("Error submitting extrinsic for {user_name} at order {i}: {:?}", e);
                continue;
                // 不增加 i，稍后按节奏重试
                // 失败不多 sleep，否则频率被改变
            }
        }

        if i % (n / 10) == 0 {
            let elapsed = start.elapsed().as_millis();
            info!("{user_name} submitted {i} orders, cost {} ms", elapsed);
            start = Instant::now();
        }
    }
    Ok(())
}

async fn place_order_no_wait_response(
    user_name: &str,
    user: &Keypair,
    subaccount: H160,
    market_id: u16,
    is_long: bool,
    price: u128,
    order_type: OrderType,
    n: u32,
    rate: u32,
) -> anyhow::Result<()> {
    let api = get_api().await?;

    let call = node_runtime::tx().perp_market().place_order(
        subaccount,
        market_id,
        is_long,
        10_000,
        price,
        order_type,
        None,
        2,
        None,
        None,
        false,
        PostOnlyParam::None,
    );

    let mut nonce = api.tx().account_nonce(&user.public_key().to_account_id()).await?;

    let mut start = Instant::now();
    let interval = Duration::from_millis(1000 / rate as u64);
    let mut next_tick = Instant::now();

    let mut i = 1;
    while i <= n {
        // 等到下一次发送时刻
        tokio::time::sleep_until(next_tick.into()).await;
        next_tick += interval;

        let params = SubstrateExtrinsicParamsBuilder::new().nonce(nonce).build();

        match api.tx().sign_and_submit(&call, user, params).await {
            Ok(_) => {
                nonce += 1;
                i += 1;
            }
            Err(e) => {
                warn!("Error submitting extrinsic for {user_name} at order {i}: {:?}", e);
                // 不增加 i，稍后按节奏重试
                // 失败不多 sleep，否则频率被改变
                nonce = api.tx().account_nonce(&user.public_key().to_account_id()).await?;
            }
        }

        if i % (n / 10) == 0 {
            let elapsed = start.elapsed().as_millis();
            info!("{user_name} submitted {} orders for market {market_id}, cost {} ms", n / 10, elapsed);
            start = Instant::now();
        }
    }

    Ok(())
}

async fn enable_spot_margin_trading(user: &Keypair, subaccount: H160) -> anyhow::Result<()> {
    let api = get_api().await?;

    let tx_enable = node_runtime::tx().subaccount().set_spot_margin(subaccount, true);

    let _events = api
        .tx()
        .sign_and_submit_then_watch_default(&tx_enable, user)
        .await?
        .wait_for_finalized_success()
        .await?;

    Ok(())
}

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

async fn get_market_id_by_name(market_name: &str) -> anyhow::Result<u16> {
    let api = get_api().await?;
    let query = node_runtime::storage().perp_market().perp_market_list();

    let result = api.storage().at_latest().await?.fetch(&query).await?;
    if let Some(markets) = result {
        for (market_id, _, _) in markets {
            let query = node_runtime::storage().perp_market().perp_markets(market_id);
            let detail = api.storage().at_latest().await?.fetch(&query).await?.unwrap();
            if String::from_utf8(detail.name.clone())? == market_name {
                return Ok(market_id);
            }
        }
    }
    Err(anyhow::anyhow!("market not found"))
}

async fn deposit(kp: &Keypair, subaccount: &H160, market_id: u8, asset: &str, amount: u128) -> anyhow::Result<()> {
    let api = get_api().await?;

    let tx_deposit = node_runtime::tx()
        .lending()
        .deposit(None, *subaccount, market_id, BoundedVec(asset.as_bytes().to_vec()), amount);

    let events = api
        .tx()
        .sign_and_submit_default(&tx_deposit, kp)
        .await?;

    // let events = api
    //     .tx()
    //     .sign_and_submit_then_watch_default(&tx_deposit, kp)
    //     .await?
    //     .wait_for_finalized_success()
    //     .await?;

    // if let Some(event) = events.find_first::<node_runtime::lending::events::Deposit>()? {
    //     debug!("Deposit success: {}", hex::encode(&event.who.0));
    // } else {
    //     return Err(anyhow::anyhow!("Failed to deposit"));
    // }
    Ok(())
}

async fn get_first_subaccount_ensure_exist(user: &Keypair, subaccount_name: &str) -> anyhow::Result<H160> {


    let api = get_api().await?;
    let mut nonce = api.tx().account_nonce(&user.public_key().to_account_id()).await?;
    let call = node_runtime::tx()
        .subaccount()
        .initialize_subaccount(BoundedVec(subaccount_name.as_bytes().to_vec()));
    let rpc_client = RpcClient::from_url(NODE_WS_ADDR).await?;
    let rpc = LegacyRpcMethods::<EthRuntimeConfig>::new(rpc_client);
    let params = SubstrateExtrinsicParamsBuilder::new().nonce(nonce).build();
    let signed_tx = api.tx().create_partial_offline(&call, params)?.sign(user);
    let call_bytes = Bytes::from_owner(signed_tx.into_encoded());
    match rpc.author_submit_extrinsic(&call_bytes).await {
        Ok(_) => {
            debug!("subaccount init success: {}", subaccount_name);
        }
        Err(e) => {
            warn!("Failed to create subaccount");
        }
    }
    Ok(Default::default())

    // let tx_init_subaccount = node_runtime::tx()
    //     .subaccount()
    //     .initialize_subaccount(BoundedVec(subaccount_name.as_bytes().to_vec()));

    // let events = api
    //     .tx()
    //     .sign_and_submit_then_watch_default(&tx_init_subaccount, user)
    //     .await?
    //     .wait_for_finalized_success()
    //     .await?;
    //
    // if let Some(event) = events.find_first::<node_runtime::subaccount::events::NewUserRecord>()? {
    //     debug!("subaccount init success: {}", subaccount_name);
    //     Ok(event.subaccount)
    // } else {
    //     Err(anyhow::anyhow!("Failed to create subaccount"))
    // }
}

async fn prepare_env(market_num: u32) -> anyhow::Result<Vec<u16>> {
    let mut market_id = Vec::new();
    // Create lending market
    let market_name = "Test_Market";
    let call = node_runtime::Call::Lending(node_runtime::lending::Call::create_market {
        market_id: 1,
        market_name: BoundedVec(market_name.as_bytes().to_vec()),
        liquidation_bonus: FixedU128(1_000_000_000_000_000_000), // 1.0
    });

    let tx = node_runtime::tx().sudo().sudo(call);

    let api = get_api().await?;
    let event = api
        .tx()
        .sign_and_submit_then_watch_default(&tx, &*ROOTER)
        .await?
        .wait_for_finalized_success()
        .await?
        .find_first::<node_runtime::lending::events::CreateMarket>()?;

    if let Some(_event) = event {
        info!("Lending market created success: {}", market_name);
    } else {
        return Err(anyhow::anyhow!("Failed to create lending market"));
    }

    // Token addresses
    let usdt_token_address = H160([1u8; 20]);
    create_lending_pool("USDT", 6, 1_000_000_000_000_000_000, 1_000_000_000_000_000_000).await?;
    create_perp_market(
        "QUOTE",
        "USDT",
        usdt_token_address,
        6,
        "NULL",
        usdt_token_address,
        6,
        "quote",
        1_000_000, // 50 USDT
        1,
        1,
    )
        .await?;
    for i in 1..=market_num {
        let token_address = H160([i as u8; 20]);
        let token_name = format!("Token{}", i);
        create_spot_market(
            (token_name.clone() + "_USDT").as_str(),
            usdt_token_address,
            "USDT",
            6,
            token_address,
            &token_name,
            8,
            1 + i as u16, // btc market index
            1, // usdt quote index
        )
            .await?;

        // Create lending pools
        create_lending_pool(&token_name, 8, 1_000_000_000_000_000_000, 1_000_000_000_000_000_000).await?;
        // Create perp markets
        create_perp_market(
            (token_name.clone() + "_USDT").as_str(),
            &token_name,
            token_address,
            8,
            "NULL",
            usdt_token_address,
            6,
            "quote",
            50_000_000, // 50 USDT
            i as u16 + 1,
            1,
        )
            .await?;

        market_id.push(i as u16 + 1);
    }

    Ok(market_id)
}

// Helper function to create spot orderbook
async fn create_spot_market(
    name: &str,
    quote_address: subxt::utils::H160,
    quote_symbol: &str,
    quote_decimal: u8,
    base_address: subxt::utils::H160,
    base_symbol: &str,
    base_decimal: u8,
    market_index: u16,
    quote_index: u16,
) -> anyhow::Result<()> {
    tokio::time::sleep(Duration::from_millis(400)).await;

    let api = get_api().await?;

    let call = node_runtime::Call::SpotMarket(node_runtime::spot_market::Call::create_spot_market {
        name: name.as_bytes().to_vec(),
        quote_address,
        quote_symbol: quote_symbol.as_bytes().to_vec(),
        quote_decimal,
        base_address,
        base_symbol: base_symbol.as_bytes().to_vec(),
        base_decimal,
        min_order_size: 1,
        tick_size: 1,
        step_size: 1,
        taker_fee_rate: 1,
        maker_fee_rate: 1,
        market_index,
        quote_index,
        recipient: Default::default(),
    });

    let tx = node_runtime::tx().sudo().sudo(call);

    let event = api
        .tx()
        .sign_and_submit_then_watch_default(&tx, &*ROOTER)
        .await?
        .wait_for_finalized_success()
        .await?
        .find_first::<node_runtime::spot_market::events::CreateSpotMarket>()?;

    if let Some(_event) = event {
        info!("Spot market {}_{} created success", base_symbol, quote_symbol);
    } else {
        return Err(anyhow::anyhow!("Failed to create spot market {}_{}", base_symbol, quote_symbol));
    }

    Ok(())
}

// Helper function to create lending pool
async fn create_lending_pool(asset: &str, decimal: u32, initial_asset_weight: u128, maintenance_asset_weight: u128) -> anyhow::Result<()> {
    tokio::time::sleep(Duration::from_millis(400)).await;

    let api = get_api().await?;

    let call = node_runtime::Call::Lending(node_runtime::lending::Call::create_pool {
        market_id: 1,
        asset: BoundedVec(asset.as_bytes().to_vec()),
        decimal,
        borrow_curve: node_runtime::runtime_types::pallet_lending::types::BorrowRateCurve {
            base_rate: FixedU128(1_000_000_000_000_000_000), // 1.0
            kinks: BoundedVec(vec![]),
            max_rate_at_full_util: FixedU128(1_500_000_000_000_000_000), // 1.5
        },
        reserve_factor: FixedU128(1_000_000_000_000_000_000),           // 1.0
        custom_liquidation_bonus: FixedU128(1_000_000_000_000_000_000), // 1.0
        initial_asset_weight: FixedU128(initial_asset_weight),
        maintenance_asset_weight: FixedU128(maintenance_asset_weight),
        initial_borrow_weight: FixedU128(1_000_000_000_000_000),     // 1.0
        maintenance_borrow_weight: FixedU128(1_000_000_000_000_000), // 1.0
    });

    let tx = node_runtime::tx().sudo().sudo(call);

    let event = api
        .tx()
        .sign_and_submit_then_watch_default(&tx, &*ROOTER)
        .await?
        .wait_for_finalized_success()
        .await?
        .find_first::<node_runtime::lending::events::CreatePool>()?;

    if let Some(_event) = event {
        info!("Lending pool {} created success", asset);
    } else {
        return Err(anyhow::anyhow!("Failed to create lending pool {}", asset));
    }

    Ok(())
}

// Helper function to create perp market
async fn create_perp_market(
    name: &str,
    base_symbol: &str,
    base_address: H160,
    base_decimal: i32,
    quote_symbol: &str,
    quote_address: H160,
    quote_decimal: i32,
    network: &str,
    oracle_price: u128,
    market_id: u16,
    quote_market_id: u16,
) -> anyhow::Result<()> {
    tokio::time::sleep(Duration::from_millis(400)).await;

    let api = get_api().await?;

    let call = node_runtime::Call::PerpMarket(node_runtime::perp_market::Call::create_market {
        market: types::create_market::Market {
            id: market_id,
            name: name.as_bytes().to_vec(),
            base_symbol: base_symbol.as_bytes().to_vec(),
            base_address,
            base_decimal,
            quote_market_id,
            quote_symbol: quote_symbol.as_bytes().to_vec(),
            quote_address,
            quote_decimal,
            network: network.as_bytes().to_vec(),
            height: 1,
            funding_rate: 10_000_000_000_000_000, // 1%
            last_cacl_funding_rate_time: 1000,
            oracle_price,
            mark_price: oracle_price,
            max_deviation_bps: u64::MAX,
            liquid_spread_bps: 10000,
            initial_margin_ratio: 500,     // 5%
            maintenance_margin_ratio: 200, // 2%
            max_active_orders: MAX_ACTIVE_ORDERS,
            is_quote_market: false,
            taker_fee_rate: 20_000,
            maker_fee_rate: 10_000,
            min_value_to_part_close: 0,
            part_close_ratio: 0,
            order_spec: MarketSpec {
                min_order_size: 1,
                tick_size: 1,
                step_size: 1,
            },
            open_interest: 0,
            long_open_pos_num: 0,
            short_open_pos_num: 0,
            base_interest_rate: FixedI128(100_000_000_000_000i128),          // 0.01%
            impact_margin_value: 100_000_000,                                // 100u
            funding_rate_change_cap: FixedI128(2_000_000_000_000_000i128),   // 0.2%
            funding_rate_change_floor: FixedI128(2_000_000_000_000_000i128), // 0.2%
        },
    });

    let tx = node_runtime::tx().sudo().sudo(call);

    let mut client = api
        .tx();
    let event = client
        .sign_and_submit_then_watch_default(&tx, &*ROOTER)
        .await?
        .wait_for_finalized_success()
        .await?
        .find_first::<node_runtime::perp_market::events::MarketCreated>()?;

    if let Some(_event) = event {
        info!("Perp market {} created success", name);
    } else {
        return Err(anyhow::anyhow!("Failed to create perp market {}", name));
    }

    let call = node_runtime::Call::Oracle(node_runtime::oracle::Call::update_oracle_price_directly {
        symbol: base_symbol.to_uppercase().as_bytes().to_vec(),
        price: oracle_price * 10u128.pow(12),
    });
    let tx = node_runtime::tx().sudo().sudo(call);
    let mut client = api
        .tx();
    let _event = client
        .sign_and_submit_then_watch_default(&tx, &*ROOTER)
        .await?
        .wait_for_finalized_success()
        .await?
        .find_first::<node_runtime::perp_market::events::MarketCreated>()?;

    Ok(())
}

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

pub fn build_epi1559_tx_to_v2(tx: ethereum::EIP1559Transaction, signer: &Keypair) -> anyhow::Result<TransactionV2> {
    let tx = ethereum::EIP1559TransactionMessage::from(tx);
    let sk = signer.clone().secret_key();
    let secret = secp256k1::SecretKey::from_byte_array(&sk.into())?;
    let signing_message = secp256k1::Message::from_digest(tx.hash().to_fixed_bytes());

    let signature = secp256k1::Secp256k1::new().sign_ecdsa_recoverable(&signing_message, &secret);

    let (recid, rs) = signature.serialize_compact();

    let r = Hash::from_slice(&rs[0..32]);
    let s = Hash::from_slice(&rs[32..64]);

    Ok(TransactionV2::EIP1559(EIP1559Transaction {
        chain_id: tx.chain_id,
        nonce: U256(tx.nonce.0),
        max_priority_fee_per_gas: U256(tx.max_priority_fee_per_gas.0),
        max_fee_per_gas: U256(tx.max_fee_per_gas.0),
        gas_limit: U256(tx.gas_limit.0),
        action: match tx.action {
            ethereum::TransactionAction::Call(addr) => TransactionAction::Call(addr.0.into()),
            _ => return Err(anyhow::anyhow!("Transaction action unknown")),
        },
        value: U256(tx.value.0),
        input: tx.input,
        access_list: vec![],
        odd_y_parity: recid != RecoveryId::Zero,
        r,
        s,
    }))
}

pub async fn create_extra_test_accounts(n: u32) -> anyhow::Result<Vec<AccountDetail>> {
    info!("Creating extra test accounts with {} items", n);
    let mut result = Vec::new();
    let mut client = get_api()
        .await?;
    let rpc_client = RpcClient::from_url(NODE_WS_ADDR).await?;
    let rpc = LegacyRpcMethods::<EthRuntimeConfig>::new(rpc_client);
    let mut nonce = client.tx().account_nonce(&ROOTER.public_key().to_account_id()).await?;
    let root_kp = dev::alith();
    for i in 1..=n {
        let addr_idx = 1000 + i;

        let kp = Keypair::from_phrase(&bip39::Mnemonic::from_str(DEV_PHRASE)?, None, DerivationPath::eth(0, addr_idx))?;

        let call = node_runtime::tx().quota().activate_account(
            kp.public_key().to_account_id()
        );


        // let _event = client
        //     .sign_and_submit_then_watch_default(&call, &*ROOTER)
        //     .await?
        //     .wait_for_finalized_success()
        //     .await?
        //     .find_first::<node_runtime::quota::events::Activate>()?
        //     .ok_or_else(|| anyhow::anyhow!("Activate event not found"))?;

        let params = SubstrateExtrinsicParamsBuilder::new().nonce(nonce).build();
        let signed_tx = client.tx().create_partial_offline(&call, params)?.sign(&root_kp);
        let call_bytes = Bytes::from_owner(signed_tx.into_encoded());
        match rpc.author_submit_extrinsic(&call_bytes).await {
            Ok(_) => {
                nonce += 1;
            }
            Err(e) => {
                warn!("Error submitting activate_account");
                continue;
            }
        }

        let tx = node_runtime::Call::Balances(node_runtime::balances::Call::force_set_balance {
            who: kp.public_key().to_account_id(),
            new_free: 10_000_000_000_000_000_000_000u128.into(),
        });
        let call = node_runtime::tx().sudo().sudo(
            tx
        );

        // let _event = client
        //     .sign_and_submit_then_watch_default(&call, &*ROOTER)
        //     .await?
        //     .wait_for_finalized_success()
        //     .await?
        //     .find_first::<node_runtime::balances::events::BalanceSet>()?
        //     .ok_or_else(|| anyhow::anyhow!("BalanceSet event not found"))?;

        let params = SubstrateExtrinsicParamsBuilder::new().nonce(nonce).build();
        let signed_tx = client.tx().create_partial_offline(&call, params)?.sign(&root_kp);
        let call_bytes = Bytes::from_owner(signed_tx.into_encoded());
        match rpc.author_submit_extrinsic(&call_bytes).await {
            Ok(_) => {
                nonce += 1;
            }
            Err(e) => {
                warn!("Error submitting BalanceSet");
                continue;
            }
        }
        let name = if i == 1 || i == 2 {
            format!("extra_pending_user{addr_idx}")
        } else {
            format!("test_user_{addr_idx}")
        };
        info!("[{i}] account init");
        let account_detail = AccountDetail { name, kp, subaccount: Default::default() };
        result.push(account_detail);
    }

    Ok(result)
}

fn target_price(user_name: &str, is_long: bool, pre_price: u128, match_price: u128, cancel_id: u32, skip_cancel: &mut bool) -> u128 {
    if user_name.starts_with("extra_pending_user") {
        return if is_long {
            pre_price * 100 // 排在最前面
        } else {
            1 // 排在最前面
        }
    }
    let price = if MATCHED_PERCENT > 0 {
        let trigger_match = 100 / MATCHED_PERCENT;
        if cancel_id >= 100 {
            if cancel_id % trigger_match == 0 {
                *skip_cancel = true;
                match_price
            } else {
                pre_price
            }
        } else {
            pre_price
        }
    } else {
        pre_price
    };
    price
}

#[derive(Clone)]
pub struct AccountDetail {
    name: String,
    kp: Keypair,
    subaccount: H160,
}

impl AccountDetail {
    pub fn new(name: String, kp: Keypair, subaccount: H160) -> Self {
        Self { name, kp, subaccount }
    }
}
