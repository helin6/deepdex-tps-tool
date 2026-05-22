#!/usr/bin/env bash
# 部署 RootDepositMulticall（需已安装 Foundry: https://book.getfoundry.sh/）
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
CONTRACTS="$ROOT/contracts"

if ! command -v forge >/dev/null 2>&1; then
  echo "未找到 forge，请先安装 Foundry: curl -L https://foundry.paradigm.xyz | bash && foundryup"
  exit 1
fi

if [[ -z "${ROOTER_PRIVATE_KEY:-}" ]]; then
  echo "请设置 ROOTER_PRIVATE_KEY（与 rooter_deposit 中 ROOTER 相同私钥，0x 前缀）"
  exit 1
fi

if [[ -z "${WS_URL:-}" ]]; then
  echo "请设置 WS_URL（与 .env 中链 RPC 一致）"
  exit 1
fi

cd "$CONTRACTS"
if [[ ! -d lib/forge-std ]]; then
  forge install foundry-rs/forge-std --no-commit
fi

forge build
forge script script/DeployRootDepositMulticall.s.sol:DeployRootDepositMulticall \
  --rpc-url "$WS_URL" \
  --broadcast \
  --legacy \
  --with-gas-price 0 \
  -vvv

echo ""
echo "将日志中的合约地址写入 .env："
echo "  ROOT_DEPOSIT_MULTICALL=0x..."
