# deepdex-tps-tool（subtx-test）镜像
#
# 原理简述：
# 1. 多阶段构建：builder 阶段用 Rust 工具链编译；最终镜像只带 Debian slim + 二进制 + CA 证书，体积小。
# 2. 配置与镜像分离：不要把各机 `.env` bake 进镜像；运行时用 `docker run --env-file`、`-e` 或 `ENV_FILE` 挂载路径。
# 3. `ShardRunConfig::load`：若设 `ENV_FILE` 则读该文件；否则尝试当前工作目录 `.env`（与 dotenvy 行为一致）。
#
# 构建（仓库根目录需含 `deepx-node-metadata.scale`，与本地 `cargo build` 相同）：
#   docker build -t deepdex-tps-tool:local .
#
# 若依赖私有 git，构建机需能访问 Git（示例：用系统 git + 本机凭据）：
#   docker build --build-arg CARGO_NET_GIT_FETCH_WITH_CLI=true -t deepdex-tps-tool:local .
#
# 运行示例（每台机器自己的 env 文件）：
#   docker run --rm --env-file /path/to/that-host.env deepdex-tps-tool:local perp_bench_fence
#   docker run --rm -e ENV_FILE=/config/.env -v /opt/secrets/tps.env:/config/.env:ro deepdex-tps-tool:local perp_bench
#
# 可用二进制：perp_bench_fence perp_bench rooter_deposit testnet_test subtx-test（默认 main）

# syntax=docker/dockerfile:1
ARG RUST_VERSION=1
FROM docker.io/library/rust:${RUST_VERSION}-bookworm AS builder

ARG CARGO_NET_GIT_FETCH_WITH_CLI=true
ENV CARGO_NET_GIT_FETCH_WITH_CLI=${CARGO_NET_GIT_FETCH_WITH_CLI}

RUN apt-get update \
    && apt-get install -y --no-install-recommends git pkg-config libssl-dev ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY deepx-node-metadata.scale ./
COPY src ./src

RUN cargo build --locked --release

FROM docker.io/library/debian:bookworm-slim AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /work
ENV RUST_LOG=info

COPY --from=builder /build/target/release/perp_bench_fence /usr/local/bin/
COPY --from=builder /build/target/release/perp_bench /usr/local/bin/
COPY --from=builder /build/target/release/rooter_deposit /usr/local/bin/
COPY --from=builder /build/target/release/testnet_test /usr/local/bin/
COPY --from=builder /build/target/release/subtx-test /usr/local/bin/

# 默认入口可覆盖：`docker run ... perp_bench`
CMD ["perp_bench_fence"]
