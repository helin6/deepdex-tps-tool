//! `RootDepositMulticall.batchDeposit` 编码与批量提交。

use ethabi::{encode, Token, Uint};
use subxt::utils::H160;

/// Lending 预编译地址（与链上 `Lending.sol` 一致）
pub const LENDING_PRECOMPILE: H160 = H160([
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x04, 0x50,
]);

/// `batchDeposit(address[],bytes,uint128)` 函数选择器
pub const BATCH_DEPOSIT_SELECTOR: [u8; 4] = [0x29, 0x28, 0xf0, 0x11];

/// 编码 `RootDepositMulticall.batchDeposit` calldata。
pub fn encode_batch_deposit(subaccounts: &[H160], asset: &[u8], amount: u128) -> Vec<u8> {
    let addrs: Vec<Token> = subaccounts
        .iter()
        .map(|s| {
            let mut a = [0u8; 20];
            a.copy_from_slice(&s.0);
            Token::Address(a.into())
        })
        .collect();
    let mut out = BATCH_DEPOSIT_SELECTOR.to_vec();
    out.extend_from_slice(&encode(&[
        Token::Array(addrs),
        Token::Bytes(asset.to_vec()),
        Token::Uint(Uint::from(amount)),
    ]));
    out
}

/// 按批大小切分地址列表。
pub fn chunk_subaccounts(subs: &[H160], batch_size: usize) -> impl Iterator<Item = &[H160]> {
    let batch_size = batch_size.max(1);
    subs.chunks(batch_size)
}
