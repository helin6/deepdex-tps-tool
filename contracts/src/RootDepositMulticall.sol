// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

/// @dev DeepX Lending 预编译（与链上 `Lending.sol` 一致）
interface ILending {
    function deposit(address subaccount, bytes memory asset, uint128 amount) external;
}

/// @title RootDepositMulticall
/// @notice 由 owner（部署者 ROOTER）调用；内部 `L.deposit` 的链上 origin 为本合约地址。
/// @dev 须事先：① ROOTER 为 owner ② 向本合约转入足够 USDC（非 ROOTER 钱包）③ `rooter_deposit` 会为合约地址激活 quota。
///      单笔 batch 过大可能 EVM OOG，请用较小 `DEPOSIT_MULTICALL_BATCH_SIZE`（如 25）。
contract RootDepositMulticall {
    address public immutable lending;
    address public immutable owner;

    event BatchDeposit(uint256 count, bytes asset, uint128 amount);

    constructor(address lending_) {
        require(lending_ != address(0), "lending=0");
        lending = lending_;
        owner = msg.sender;
    }

    /// @param subaccounts 目标子账户地址列表
    /// @param asset 资产符号字节（如 `usdc`）
    /// @param amount 每个子账户存入相同数量
    function batchDeposit(
        address[] calldata subaccounts,
        bytes calldata asset,
        uint128 amount
    ) external {
        require(msg.sender == owner, "!owner");
        uint256 n = subaccounts.length;
        require(n > 0, "empty");
        ILending L = ILending(lending);
        for (uint256 i = 0; i < n; ) {
            L.deposit(subaccounts[i], asset, amount);
            unchecked {
                ++i;
            }
        }
        emit BatchDeposit(n, asset, amount);
    }
}
