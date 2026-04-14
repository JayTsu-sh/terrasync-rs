@echo off
REM make.bat for terrasync (Windows)
REM Usage: make.bat [target]

if "%1"=="" goto release
if "%1"=="debug" goto debug
if "%1"=="release" goto release
if "%1"=="gui" goto gui
if "%1"=="gui-debug" goto gui_debug
if "%1"=="gui-frontend" goto gui_frontend
if "%1"=="clean" goto clean
if "%1"=="check" goto check
if "%1"=="test" goto test
if "%1"=="fmt" goto fmt
echo Unknown target: %1
echo Available targets: debug, release, gui, gui-debug, gui-frontend, clean, check, test, fmt
goto end

:debug
echo [Building debug]
cargo build
goto end

:release
echo [Building release]
cargo build --release
goto end

:gui
echo [GUI release: frontend first, then backend]
echo [1/2] Building frontend...
cd /d %~dp0web-ui
if not exist node_modules (
  echo   Installing frontend dependencies...
  call npm install
  if errorlevel 1 (cd /d %~dp0 && echo Frontend npm install failed && goto end)
)
call npx vue-tsc -b
if errorlevel 1 (cd /d %~dp0 && echo Frontend type-check failed && goto end)
call npx vite build
if errorlevel 1 (cd /d %~dp0 && echo Frontend build failed && goto end)
cd /d %~dp0
echo [2/2] Building backend...
set "SKIP_FRONTEND_BUILD=1"
cargo build --features gui --release
set "SKIP_FRONTEND_BUILD="
if errorlevel 1 (echo Backend build failed && goto end)
echo [Build complete]

:gui_debug
echo [GUI debug: frontend first, then backend]
echo [1/2] Building frontend...
cd /d %~dp0web-ui
if not exist node_modules (
  echo   Installing frontend dependencies...
  call npm install
  if errorlevel 1 (cd /d %~dp0 && echo Frontend npm install failed && goto end)
)
call npx vue-tsc -b
if errorlevel 1 (cd /d %~dp0 && echo Frontend type-check failed && goto end)
call npx vite build
if errorlevel 1 (cd /d %~dp0 && echo Frontend build failed && goto end)
cd /d %~dp0
echo [2/2] Building backend...
set "SKIP_FRONTEND_BUILD=1"
cargo build --features gui
set "SKIP_FRONTEND_BUILD="
if errorlevel 1 (echo Backend build failed && goto end)
echo [Build complete]

:gui_frontend
cd web-ui
if not exist node_modules (
  echo [0/2] Installing frontend dependencies...
  call npm install
  if errorlevel 1 (cd .. && goto end)
)
echo [1/2] Type-checking frontend...
call npx vue-tsc -b
if errorlevel 1 (cd .. && goto end)
echo [2/2] Building frontend...
call npx vite build
cd ..
goto end

:clean
echo [Cleaning build artifacts]
cargo clean
goto end

:check
echo [Checking code]
cargo check
goto end

:test
echo [Running tests]
cargo test --workspace --no-fail-fast
goto end

:fmt
echo [Formatting code]
cargo fmt
goto end

:end
