# Makefile for terrasync
# 使用 x86_64-unknown-linux-musl 目标平台编译
#
# 目标概览：
#   frontend          仅构建前端 (npm install + type-check + vite build)
#   backend           仅构建后端 release，含 GUI（要求先执行 make frontend）
#   backend-debug     仅构建后端 debug，含 GUI（要求先执行 make frontend）
#   gui               完整构建 release（frontend + backend）
#   gui-debug         完整构建 debug（frontend + backend）
#   release           纯 CLI release（无 GUI 特性）
#   debug             纯 CLI debug（无 GUI 特性）

TARGET    := x86_64-unknown-linux-musl
BUILD_DIR := target/$(TARGET)

DEBUG_BINARY   := $(BUILD_DIR)/debug/terrasync
RELEASE_BINARY := $(BUILD_DIR)/release/terrasync

.PHONY: all frontend backend backend-debug gui gui-debug release debug clean check test fmt doc tree version

all: gui

# ── 前端 ─────────────────────────────────────────────────────────────────────

frontend:
	@if [ ! -d web-ui/node_modules ]; then \
		echo "[frontend] npm install..."; \
		cd web-ui && npm install || exit 1; \
	fi
	@echo "[frontend] Building..."
	@cd web-ui && npm run build || exit 1

# ── 后端（含 GUI 特性）───────────────────────────────────────────────────────
# 跳过前端重建，要求 dist/ 已存在（先执行 make frontend）

backend:
	@echo "[backend] Building release (musl)..."
	SKIP_FRONTEND_BUILD=1 cargo zigbuild --target $(TARGET) --features gui --release
	@echo "[backend] → $(RELEASE_BINARY)"

backend-debug:
	@echo "[backend] Building debug (musl)..."
	SKIP_FRONTEND_BUILD=1 cargo zigbuild --target $(TARGET) --features gui
	@echo "[backend] → $(DEBUG_BINARY)"

# ── 完整构建（frontend + backend）────────────────────────────────────────────

gui: frontend backend

gui-debug: frontend backend-debug

# ── 纯 CLI（无 GUI 特性）─────────────────────────────────────────────────────

release:
	cargo zigbuild --target $(TARGET) --release

debug:
	cargo zigbuild --target $(TARGET)

# ── 工具 ─────────────────────────────────────────────────────────────────────

clean:
	cargo clean

check:
	cargo check

test:
	cargo test --workspace --no-fail-fast

fmt:
	cargo fmt

doc:
	cargo doc

tree:
	cargo tree

version:
	@echo "terrasync version: $(shell grep 'version =' Cargo.toml | head -n 1 | cut -d '"' -f 2)"
