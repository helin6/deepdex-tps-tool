// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

/// @dev DeepX Lending 预编译（与链上 `Lending.sol` 一致）
interface ILending {
    function deposit(address subaccount, bytes memory asset, uint128 amount) external;
}

/// @title RootDepositMulticall
/// @notice 由资金账户（部署者）在一笔 EVM 交易内对多个子账户批量 `deposit`，降低 ROOTER 逐笔 Substrate deposit 的 nonce/RPC 开销。
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
