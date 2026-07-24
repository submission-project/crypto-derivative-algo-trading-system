.PHONY: setup dev test-common test-schemas test-all build-rust \
       collect-binance-trades collect-binance-ticker collect-binance-kline \
       collect-okx-trades collect-okx-ticker collect-okx-kline \
       collect-binance collect-okx collect-all \
       run-api-server-dev \
       backtest-demo backtest-live backtest-walkforward backtest-oi \
       test-backtest test-research test-quant

ENV ?= dev

include .env.$(ENV)
export

setup:
	uv sync
	pnpm install

dev-up:
	ENV_FILE=../.env.$(ENV) docker-compose -f infra/docker-compose.yml up -d


dev-down:
	ENV_FILE=../.env.$(ENV) docker-compose -f infra/docker-compose.yml down -v

test-all: test-common test-schemas test-rust

test-common:
	APP_NODE_ID=$(APP_NODE_ID) pytest packages/common/tests/

test-schemas:
	APP_NODE_ID=$(APP_NODE_ID) pytest packages/schemas/tests/

test-rust:
	cd crates/rust_core && cargo test --no-default-features -- --nocapture

build-rust:
	cd crates/rust_core && .venv/bin/maturin develop

# ── Collectors ──
run-collector-producer-binance-perp-aggtrade:
	rpk connect run --env-file .env.$(ENV) apps/collectors/src/exchange/binance/perp/yaml/aggtrade.yaml 

run-collector-producer-binance-perp-trade:
	rpk connect run --env-file .env.$(ENV) apps/collectors/src/exchange/binance/perp/yaml/trade.yaml 

collect-okx-kline:
	rpk connect run --env-file .env.$(ENV) apps/collectors/src/exchange/okx/perp/yaml/kline_1m.yaml

collect-binance: collect-binance-trades collect-binance-ticker collect-binance-kline

# rust-package
build-rust-core_app-test-package: 
	cd apps/test && uv add "../../crates/rust_core"

build-rust-core_app-stream_processor-package: 
	cd apps/stream_processor && uv add "../../crates/rust_core" 

run-collector-producer-binance-spot-trades:
	cd apps/collectors/src/exchange/binance/spot/python && uv run src/binance_spot_sbe/main.py

run-collector-binance-perp-trade:
	ENV_FILE=.env.$(ENV) PYTHONPATH=apps/collectors/src/exchange/binance/perp/python/src:packages/common/src:packages/schemas/src:packages/storage/src:packages/messaging/src \
		uv run python -m binance_perp_collector.main
		
test-collector-consumer-binance-perp-trades:
	rpk topic consume ${BINANCE_PERP_TOPIC_TRADES}

test-collector-consumer-binance-perp-aggtrades:
	rpk topic consume ${BINANCE_PERP_TOPIC_AGGTRADES}

test-collector-consumer-binance-spot-trades:
	rpk topic consume ${BINANCE_SPOT_TOPIC_TRADES}

test-collect:
	@mkdir -p .scratch
	@sed '/^output:/,$$d' $(YAML) > .scratch/_test_connect.yaml
	@echo "output:" >> .scratch/_test_connect.yaml
	@echo "  stdout: {}" >> .scratch/_test_connect.yaml
	rpk connect run --env-file .env.$(ENV) .scratch/_test_connect.yaml


# ── Canonical Pipeline (QuestDB + Redis) ──
run-canonical-pipeline:
	ENV_FILE=.env.$(ENV) uv run python -m stream_processor.canonical_pipeline

# ===== run python test =====
run-python:
	ENV_FILE=.env.$(ENV) uv run $(PYTHON_PATH)

# API Server (`api_server.main:app`): dev env + `--reload`
run-api-server-dev:
	@test -f .env.dev || (echo "오류: .env.dev 파일이 레포지토리 루트에 필요합니다."; exit 1)
	bash -c '\
		set -a && \
		. ./.env.dev && \
		set +a && \
		export ENV_FILE="$$(pwd)/.env.dev" && \
		uv run uvicorn api_server.main:app --reload \
			--reload-dir apps/api_server/src \
			--reload-dir packages/common/src \
			--reload-dir packages/schemas/src \
			--reload-dir packages/storage/src \
			--reload-dir packages/messaging/src \
			--reload-dir apps/execution_gateway/src \
			--reload-dir apps/collectors/src \
			--host "$$API_HOST" \
			--port "$$API_PORT"'

run-pytest:
	ENV_FILE=.env.$(ENV) uv run pytest $(PYTHON_PATH) -v -s


# Pytest Integration
run-pytest-integration:
	ENV_FILE=.env.$(ENV) APP_NODE_ID=$${APP_NODE_ID:-1} uv run pytest -m integration -v -s


# uv sync --reinstall-package rust-core

install-package:
	uv add --package ${DIR} ${PACKAGE}

# Code Static Analysis
code-static-analysis:
	uv run mypy $(PYTHON_PATH)


# ── Backtesting & Research ──
run-backtest:
	ENV_FILE=.env.$(ENV) DATA_PATHS="$(DATA_PATHS)" uv run pytest $(PYTHON_PATH) -v -s

# Demo backtest (no network, hardcoded data)
backtest-demo:
	ENV_FILE=.env.$(ENV) PYTHONPATH=.:packages/risk/src \
		uv run python -m research.backtests.microstructure.backtest demo

# Live data backtest (fetches from Binance public API)
backtest-live:
	ENV_FILE=.env.$(ENV) PYTHONPATH=.:packages/risk/src \
		uv run python -m research.backtests.microstructure.backtest live

# Walk-Forward optimization backtest
backtest-walkforward:
	ENV_FILE=.env.$(ENV) PYTHONPATH=.:packages/risk/src \
		uv run python -m research.backtests.microstructure.backtest walkforward

# Aligned OI Box + Microstructure backtest
backtest-oi:
	ENV_FILE=.env.$(ENV) PYTHONPATH=.:packages/risk/src \
		uv run python -m research.backtests.microstructure.backtest oi

# Test backtest modules
test-backtest:
	ENV_FILE=.env.$(ENV) PYTHONPATH=.:packages/risk/src \
		uv run pytest research/backtests/ -v -s

# Test research modules
test-research:
	ENV_FILE=.env.$(ENV) PYTHONPATH=.:packages/risk/src:research \
		uv run pytest research/ -v -s

# Run all quant tests (backtest + research + risk)
test-quant: test-backtest test-research
	ENV_FILE=.env.$(ENV) uv run pytest packages/risk/tests/ -v -s
