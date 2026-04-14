# 构建命令：docker build -t terrasync -f docker/Dockerfile .
# 运行命令：docker run --rm terrasync

#####################################################################
## 第一阶段：编译 Rust 程序
#####################################################################
FROM rust:alpine AS builder

# 安装编译所需的工具链
RUN apk update && \
    apk upgrade --no-cache && \
    apk add --no-cache lld musl musl-dev clang git make ca-certificates

WORKDIR /app

# 依赖缓存层：先只复制 Cargo 配置文件，预编译依赖
COPY Cargo.toml Cargo.lock ./
COPY crates/utils/Cargo.toml crates/utils/Cargo.toml
COPY crates/cli/Cargo.toml crates/cli/Cargo.toml
COPY crates/app/Cargo.toml crates/app/Cargo.toml
COPY crates/db/Cargo.toml crates/db/Cargo.toml
COPY crates/kafka/Cargo.toml crates/kafka/Cargo.toml
COPY crates/storage_v2/Cargo.toml crates/storage_v2/Cargo.toml
COPY crates/licensing/Cargo.toml crates/licensing/Cargo.toml
COPY crates/transport/Cargo.toml crates/transport/Cargo.toml
COPY crates/sync-delta/Cargo.toml crates/sync-delta/Cargo.toml
COPY crates/web/Cargo.toml crates/web/Cargo.toml

# 创建空 src 让 cargo 能解析 workspace，预编译依赖
RUN mkdir -p src crates/utils/src crates/cli/src crates/app/src crates/db/src crates/kafka/src crates/storage_v2/src crates/licensing/src crates/transport/src crates/sync-delta/src crates/web/src \
    && echo "fn main() {}" > src/main.rs \
    && for d in utils cli app db kafka storage_v2 licensing transport sync-delta web; do echo "" > crates/$d/src/lib.rs; done \
    && cargo build --release --target x86_64-unknown-linux-musl || true \
    && rm -rf src */src

# 复制实际源码并构建
COPY . .
RUN cargo build --release --target x86_64-unknown-linux-musl

#####################################################################
## 第二阶段：准备运行时必要的系统文件
#####################################################################
FROM alpine:latest AS files

# 安装证书、MIME 类型检测和时区数据
RUN apk update && \
    apk upgrade --no-cache && \
    apk add --no-cache ca-certificates mailcap tzdata

# 更新 CA 证书
RUN update-ca-certificates

# 创建非特权用户（UID 10001）
ENV USER=terrasync
ENV UID=10001
RUN adduser \
    --disabled-password \
    --gecos "" \
    --home "/nonexistent" \
    --shell "/sbin/nologin" \
    --no-create-home \
    --uid "${UID}" \
    "${USER}"

#####################################################################
## 第三阶段：构建最终的最小镜像（scratch）
#####################################################################
FROM scratch

# 复制用户信息和 MIME 类型文件
COPY --from=files --chmod=444 \
    /etc/passwd \
    /etc/group \
    /etc/mime.types \
    /etc/

# 复制 CA 根证书（HTTPS 必需）
COPY --from=files --chmod=444 /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

# 复制时区数据
COPY --from=files --chmod=444 /usr/share/zoneinfo /usr/share/zoneinfo

# 复制编译好的可执行文件
COPY --from=builder /app/target/x86_64-unknown-linux-musl/release/terrasync /usr/local/bin/terrasync

# 使用非特权用户运行，降低容器被攻破后的危害
USER terrasync:terrasync

WORKDIR /app
ENTRYPOINT ["/usr/local/bin/terrasync"]
