# deepdex-tps-tool（subtx-test）镜像
#
# 必须使用 BuildKit（否则不支持 `--mount=type=secret`，且会退回 legacy builder）：
#   export DOCKER_BUILDKIT=1
#   # 或一次性：
#   DOCKER_BUILDKIT=1 docker build ...
#
# 若 Cargo 依赖 GitHub **私有** HTTPS 仓库，须注入只读 PAT（勿提交到 Git）：
#   1) 创建 $HOME/.docker-github-netrc（chmod 600），内容：
#        machine github.com
#        login x-access-token
#        password ghp_你的PAT
#   2) 构建：
#        DOCKER_BUILDKIT=1 docker build \
#          --secret id=git_netrc,src=$HOME/.docker-github-netrc \
#          -t deepdex-tps-tool:local .
#
# 未传 --secret 时仍可构建（仅当所有 git 依赖均为公开可读）；私有库不传会报
# `could not read Username` 或 `revision ... not found`（多为未拉到私有提交）。
#
# 服务器长期开启 BuildKit（Ubuntu）：在 /etc/docker/daemon.json 增加
#   { "features": { "buildkit": true } }
# 然后 sudo systemctl restart docker
#
# 运行示例：docker run --rm --env-file ./.env deepdex-tps-tool:local perp_bench_fence

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

# 私有 GitHub：传 --secret id=git_netrc,...；git/cargo 会读 /root/.netrc
RUN --mount=type=secret,id=git_netrc,target=/root/.netrc,required=false \
    cargo build --locked --release

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

CMD ["perp_bench_fence"]
