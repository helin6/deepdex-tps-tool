//! Multicall deposit 走 Ethereum JSON-RPC（`eth_sendRawTransaction`），与 deepx-driver 一致，可在浏览器看到 ROOTER → 合约 的交易。

use ethereum::{EIP1559Transaction as EthEip1559, EIP1559TransactionMessage};
use secp256k1::ecdsa::RecoveryId;
use sha3::Digest;
use subxt::backend::rpc::RpcClient;
use subxt::ext::subxt_rpcs::rpc_params;
use subxt::utils::H160;
use subxt_signer::eth::Keypair;

/// 构建 EIP-1559 签名 raw 交易（type byte 0x02 + rlp），返回 `(raw, tx_hash)`。
pub fn build_signed_raw_eip1559(
    tx: EthEip1559,
    signer: &Keypair,
) -> anyhow::Result<(Vec<u8>, subxt::utils::H256)> {
    let tx_msg = EIP1559TransactionMessage::from(tx.clone());
    let sk = signer.clone().secret_key();
    let secret = secp256k1::SecretKey::from_byte_array(&sk.into())?;
    let signing_message = secp256k1::Message::from_digest(tx_msg.hash().to_fixed_bytes());
    let signature = secp256k1::Secp256k1::new().sign_ecdsa_recoverable(&signing_message, &secret);
    let (recid, rs) = signature.serialize_compact();
    let r = &rs[0..32];
    let s = &rs[32..64];
    let odd_y_parity = recid != RecoveryId::Zero;

    let eip1559_encode = EthEip1559 {
        chain_id: tx.chain_id,
        nonce: tx.nonce,
        max_priority_fee_per_gas: tx.max_priority_fee_per_gas,
        max_fee_per_gas: tx.max_fee_per_gas,
        gas_limit: tx.gas_limit,
        action: tx.action,
        value: tx.value,
        input: tx.input,
        access_list: vec![],
        odd_y_parity,
        r: primitive_types::H256::from_slice(r),
        s: primitive_types::H256::from_slice(s),
    };
    let encoded = rlp::encode(&eip1559_encode);
    let mut out = vec![0u8; 1 + encoded.len()];
    out[0] = 2;
    out[1..].copy_from_slice(&encoded);
    let tx_hash = subxt::utils::H256::from_slice(sha3::Keccak256::digest(&out).as_slice());
    Ok((out, tx_hash))
}

fn parse_hex_u64(s: &str) -> anyhow::Result<u64> {
    let s = s.strip_prefix("0x").unwrap_or(s);
    if s.is_empty() {
        return Ok(0);
    }
    u64::from_str_radix(s, 16).map_err(|e| anyhow::anyhow!("parse hex {s:?}: {e}"))
}

fn h160_to_rpc(addr: H160) -> String {
    format!("0x{}", hex::encode(addr.0))
}

/// `eth_getTransactionCount(address, latest)`。
pub async fn eth_transaction_count(rpc: &RpcClient, who: H160) -> anyhow::Result<u64> {
    let n: String = rpc
        .request(
            "eth_getTransactionCount",
            rpc_params![h160_to_rpc(who), "latest"],
        )
        .await?;
    parse_hex_u64(&n)
}

/// `eth_sendRawTransaction`，返回 tx hash。
pub async fn eth_send_raw_transaction(
    rpc: &RpcClient,
    raw: &[u8],
) -> anyhow::Result<subxt::utils::H256> {
    let hex_tx = format!("0x{}", hex::encode(raw));
    let hash: String = rpc
        .request("eth_sendRawTransaction", rpc_params![hex_tx])
        .await?;
    let s = hash.strip_prefix("0x").unwrap_or(&hash);
    let bytes = hex::decode(s)?;
    if bytes.len() != 32 {
        anyhow::bail!("eth_sendRawTransaction 返回异常 hash 长度: {}", bytes.len());
    }
    Ok(subxt::utils::H256::from_slice(&bytes))
}

pub struct EvmReceipt {
    pub success: bool,
    pub block_number: Option<u64>,
}

/// 轮询 `eth_getTransactionReceipt` 直到进块或超时。
pub async fn wait_transaction_receipt(
    rpc: &RpcClient,
    tx_hash: subxt::utils::H256,
) -> anyhow::Result<EvmReceipt> {
    let timeout_ms = std::env::var("ROOTER_EVM_RECEIPT_WAIT_MS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(30_000);
    let poll_ms = std::env::var("ROOTER_EVM_RECEIPT_POLL_MS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(500)
        .max(200);
    let hash_rpc = format!("0x{}", hex::encode(tx_hash.0));
    let deadline = std::time::Instant::now() + std::time::Duration::from_millis(timeout_ms);

    loop {
        let receipt: Option<serde_json::Value> = rpc
            .request("eth_getTransactionReceipt", rpc_params![hash_rpc.clone()])
            .await?;
        if let Some(r) = receipt {
            let status = r
                .get("status")
                .and_then(|v| v.as_str())
                .unwrap_or("0x0");
            let success = status == "0x1" || status == "0x01";
            let block_number = r
                .get("blockNumber")
                .and_then(|v| v.as_str())
                .and_then(|s| parse_hex_u64(s).ok());
            return Ok(EvmReceipt {
                success,
                block_number,
            });
        }
        if std::time::Instant::now() >= deadline {
            anyhow::bail!(
                "交易 {hash_rpc} 在 {}ms 内无 receipt（未进块）；请用 eth_getTransactionReceipt 自查",
                timeout_ms
            );
        }
        tokio::time::sleep(std::time::Duration::from_millis(poll_ms)).await;
    }
}
