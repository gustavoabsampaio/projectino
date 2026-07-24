# Hybrid dev workflow: infrastructure in Docker, everything else native.
# JS toolchain is Bun only (no Node/npm).

COMPOSE := docker compose

.PHONY: help infra-up infra-down infra-logs topics lake-reset build test lint fmt \
        test-ingestor run-ingestor backfill run-hot run-cold run-api \
        frontend-install frontend-dev \
        module-build module-publish module-republish module-generate module-generate-rust

help: ## list targets
	@grep -E '^[a-zA-Z_-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-18s %s\n", $$1, $$2}'

infra-up: ## start Redpanda, MinIO, SpacetimeDB
	$(COMPOSE) up -d

infra-down: ## stop infrastructure
	$(COMPOSE) down

infra-logs: ## tail infrastructure logs
	$(COMPOSE) logs -f

topics: ## create market topics + their .dlq siblings with explicit configs (idempotent)
	for t in market.trades market.book-tickers market.klines; do \
		$(COMPOSE) exec redpanda rpk topic create $$t --partitions 6 \
			--topic-config retention.ms=259200000 \
			--topic-config cleanup.policy=delete || true; \
		$(COMPOSE) exec redpanda rpk topic create $$t.dlq --partitions 6 \
			--topic-config retention.ms=86400000 \
			--topic-config cleanup.policy=delete || true; \
	done

lake-reset: ## empty the MinIO lake bucket (needed after a lake schema change)
	$(COMPOSE) run --rm minio-init \
		'mc alias set local http://minio:9000 minioadmin minioadmin && \
		 mc rm --recursive --force local/market-lake/ ; \
		 mc mb --ignore-existing local/market-lake && \
		 echo "lake bucket market-lake emptied"'

build: ## build the Rust workspace and install frontend deps
	cargo build --workspace
	cd frontend && bun install

test: ## run Rust tests
	cargo test --workspace

lint: ## fmt check + clippy + frontend typecheck
	cargo fmt --all --check
	cargo clippy --workspace --all-targets -- -D warnings
	cd frontend && bun run typecheck

fmt: ## format Rust code
	cargo fmt --all

test-ingestor: ## run ingestor tests + fixture replays (hermetic), log to logs/
	./scripts/test-ingestor.sh

run-ingestor: ## run the Binance → Kafka ingestor natively
	cargo run -p ingestor

backfill: ## pull historical klines from Binance REST into market.klines
	cargo run -p ingestor --bin backfill

run-hot: ## run the hot-path consumer natively
	cargo run -p hot-consumer

run-cold: ## run the cold-path consumer natively
	cargo run -p cold-consumer

run-api: ## run the Axum historical API natively
	cargo run -p api

frontend-install: ## bun install (generates/updates bun.lock — commit it)
	cd frontend && bun install

frontend-dev: ## run the Vite dev server with Bun
	cd frontend && bun run dev

module-build: ## compile the SpacetimeDB module to wasm (no server needed)
	spacetime build --module-path crates/spacetime-module

module-publish: ## publish the SpacetimeDB module to the local server
	spacetime publish --server http://localhost:3000 --module-path crates/spacetime-module projectino

module-republish: ## republish after a breaking schema change (DESTROYS data)
	spacetime publish --server http://localhost:3000 --module-path crates/spacetime-module --delete-data=on-conflict --yes projectino

module-generate: ## generate TypeScript bindings for the frontend
	spacetime generate --lang typescript --out-dir frontend/src/module_bindings --module-path crates/spacetime-module

module-generate-rust: ## regenerate the hot-consumer's Rust bindings (then run `cargo fmt`)
	spacetime generate --lang rust --out-dir crates/hot-consumer/src/module_bindings --module-path crates/spacetime-module
	cargo fmt -p hot-consumer
