// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

import {Script, console2} from "forge-std/Script.sol";
import {RootDepositMulticall} from "../src/RootDepositMulticall.sol";

/// @notice 部署 RootDepositMulticall，owner = 部署者（请使用 ROOTER 私钥）。
/// 用法（在 `deepdex-tps-tool/contracts` 目录）:
///   export WS_URL=ws://192.168.201.11:9928  
///   export ROOTER_PRIVATE_KEY=0x349f7f21d09265b525c562df697cee56d65fe23fe638bb890dd2213a0cca5dcd
///   forge script script/DeployRootDepositMulticall.s.sol:DeployRootDepositMulticall \
///     --rpc-url $WS_URL --broadcast --legacy --with-gas-price 0 -vvv
contract DeployRootDepositMulticall is Script {
    address constant LENDING_PRECOMPILE = 0x0000000000000000000000000000000000000450;

    function run() external {
        uint256 deployerKey = vm.envUint("ROOTER_PRIVATE_KEY");
        // DeepX 测试网 EVM 交易常用 gas_price=0（与 deepx-driver 一致）
        vm.txGasPrice(0);
        vm.startBroadcast(deployerKey);
        RootDepositMulticall mc = new RootDepositMulticall(LENDING_PRECOMPILE);
        vm.stopBroadcast();
        console2.log("RootDepositMulticall deployed at:", address(mc));
        console2.log("Lending precompile:", LENDING_PRECOMPILE);
        console2.log("Owner (ROOTER):", mc.owner());
    }
}
