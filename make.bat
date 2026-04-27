@echo off
REM make.bat for terrasync (Windows)
REM Usage: make.bat [target]
REM
REM Targets:
REM   frontend          仅构建前端 (npm install + type-check + vite build)
REM   backend           仅构建后端 release，含 GUI（要求先执行 make.bat frontend）
REM   backend-debug     仅构建后端 debug，含 GUI（要求先执行 make.bat frontend）
REM   gui               完整构建 release（frontend + backend）
REM   gui-debug         完整构建 debug（frontend + backend）
REM   release           纯 CLI release（无 GUI 特性）
REM   debug             纯 CLI debug（无 GUI 特性）

if "%1"==""               goto gui
if "%1"=="frontend"       goto frontend
if "%1"=="backend"        goto backend
if "%1"=="backend-debug"  goto backend_debug
if "%1"=="gui"            goto gui
if "%1"=="gui-debug"      goto gui_debug
if "%1"=="release"        goto release
if "%1"=="debug"          goto debug
if "%1"=="clean"          goto clean
if "%1"=="check"          goto check
if "%1"=="test"           goto test
if "%1"=="fmt"            goto fmt
echo Unknown target: %1
echo Available: frontend, backend, backend-debug, gui, gui-debug, release, debug, clean, check, test, fmt
goto end

REM ── 前端 ─────────────────────────────────────────────────────────────────
:frontend
call :_build_frontend
goto end

REM ── 后端（含 GUI 特性）───────────────────────────────────────────────────
:backend
echo [backend] Building release...
call :_build_backend --release
if errorlevel 1 goto end
echo [backend] Done: target\release\terrasync.exe
goto end

:backend_debug
echo [backend] Building debug...
call :_build_backend
if errorlevel 1 goto end
echo [backend] Done: target\debug\terrasync.exe
goto end

REM ── 完整构建（frontend + backend）────────────────────────────────────────
:gui
call :_build_frontend
if errorlevel 1 goto end
echo [gui] Building backend (release)...
call :_build_backend --release
if errorlevel 1 goto end
echo [gui] Release complete: target\release\terrasync.exe
goto end

:gui_debug
call :_build_frontend
if errorlevel 1 goto end
echo [gui-debug] Building backend (debug)...
call :_build_backend
if errorlevel 1 goto end
echo [gui-debug] Debug complete: target\debug\terrasync.exe
goto end

REM ── 纯 CLI（无 GUI 特性）─────────────────────────────────────────────────
:release
echo [release] Building CLI release...
cargo build --release
if errorlevel 1 (echo [release] Build failed && goto end)
goto end

:debug
echo [debug] Building CLI debug...
cargo build
if errorlevel 1 (echo [debug] Build failed && goto end)
goto end

REM ── 工具 ─────────────────────────────────────────────────────────────────
:clean
echo [clean] Cleaning build artifacts...
cargo clean
goto end

:check
echo [check] Checking code...
cargo check
goto end

:test
echo [test] Running tests...
cargo test --workspace --no-fail-fast
goto end

:fmt
echo [fmt] Formatting code...
cargo fmt
goto end

REM ── 子程序：构建前端 ─────────────────────────────────────────────────────
:_build_frontend
if not exist "%~dp0web-ui\node_modules" (
    echo [frontend] npm install...
    pushd "%~dp0web-ui"
    call npm install
    if errorlevel 1 (popd && echo [frontend] npm install failed && exit /b 1)
    popd
)
echo [frontend] Building...
pushd "%~dp0web-ui"
call npm run build
if errorlevel 1 (popd && echo [frontend] Build failed && exit /b 1)
popd
exit /b 0

REM ── 子程序：构建后端（含 GUI 特性）──────────────────────────────────────
REM %1 = "--release" 或空（debug）
:_build_backend
set "SKIP_FRONTEND_BUILD=1"
cargo build --features gui %1
set "SKIP_FRONTEND_BUILD="
if errorlevel 1 (echo [backend] Build failed && exit /b 1)
exit /b 0

:end
