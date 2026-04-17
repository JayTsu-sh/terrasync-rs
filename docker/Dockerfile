# 构建命令：docker build -t lenovonetap.io/terrasync:latest -f docker/Dockerfile .
# 运行命令：docker run --rm lenovonetap.io/terrasync:latest [子命令和参数]
#
# 设计目标：单机百万级 IOPS。使用 glibc（非 musl）+ mimalloc 以获得最佳分配器性能，
# 运行时用 distroless/cc 保持镜像最小化且无 shell 攻击面。
# distroless/cc-debian12 已内置 glibc / libgcc_s / libssl / ca-certificates / tzdata，
# 无需额外复制运行时文件。

#####################################################################
## 第一阶段：编译 Rust 程序（glibc 基础）
#####################################################################
FROM rust:bookworm AS builder

# aws-lc-rs 需要 cmake；其它为通用构建工具
RUN apt-get update && apt-get install -y --no-install-recommends \
        pkg-config cmake clang git make ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# 一次性复制源码；通过 BuildKit cache mount 加速 registry / target 复用
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/app/target \
    cargo build --release \
    && cp target/release/terrasync /usr/local/bin/terrasync

#####################################################################
## 第二阶段：distroless 运行时（含 glibc + CA + tzdata，已预置 nonroot 用户）
#####################################################################
FROM gcr.io/distroless/cc-debian12:nonroot

COPY --from=builder /usr/local/bin/terrasync /usr/local/bin/terrasync

USER nonroot:nonroot
WORKDIR /app
ENTRYPOINT ["/usr/local/bin/terrasync"]
