# Makefile for rust-terrasync
# 使用 x86_64-unknown-linux-musl 目标平台编译

# 目标平台
TARGET := x86_64-unknown-linux-musl

# 构建目录
BUILD_DIR := target/$(TARGET)

# 可执行文件路径
DEBUG_BINARY := $(BUILD_DIR)/debug/rust-terrasync
RELEASE_BINARY := $(BUILD_DIR)/release/rust-terrasync

# 默认目标
all: release

# 编译debug版本
debug:
	cargo zigbuild --target $(TARGET)

# 编译release版本
release:
	cargo zigbuild --target $(TARGET) --release

# 构建 GUI（前端 + 后端 release）
gui: gui-frontend
	cargo zigbuild --target $(TARGET) --features gui --release

# 仅构建前端
gui-frontend:
	cd web-ui && npm run build

# 清理编译产物
clean:
	cargo clean

# 检查代码
check:
	cargo check

# 运行测试
test:
	cargo test --workspace --no-fail-fast

# 格式化代码
fmt:
	cargo fmt

# 生成文档
doc:
	cargo doc

# 显示依赖树
tree:
	cargo tree

# 显示版本信息
version:
	@echo "rust-terrasync version: $(shell grep 'version =' Cargo.toml | head -n 1 | cut -d '"' -f 2)"

.PHONY: all debug release gui gui-frontend run run-release clean check test fmt fmt-check doc tree version
