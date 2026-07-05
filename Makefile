# Hybrid dev workflow: infrastructure in Docker, everything else native.
# JS toolchain is Bun only (no Node/npm).

COMPOSE := docker compose

.PHONY: help infra-up infra-down infra-logs topic-create build test lint fmt \
        run-ingestor run-hot run-cold run-api \
        frontend-install frontend-dev \
        module-publish module-generate

help: ## list targets
	@grep -E '^[a-zA-Z_-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-18s %s\n", $$1, $$2}'

infra-up: ## start Redpanda, MinIO, SpacetimeDB
	$(COMPOSE) up -d

infra-down: ## stop infrastructure
	$(COMPOSE) down

infra-logs: ## tail infrastructure logs
	$(COMPOSE) logs -f

topic-create: ## create the raw events topic (idempotent)
	$(COMPOSE) exec redpanda rpk topic create market.events.raw --partitions 3 || true

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

run-ingestor: ## run the Binance → Kafka ingestor natively
	cargo run -p ingestor

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

module-publish: ## publish the SpacetimeDB module to the local server
	spacetime publish --server http://localhost:3000 --project-path crates/spacetime-module projectino

module-generate: ## generate TypeScript bindings for the frontend
	spacetime generate --lang typescript --out-dir frontend/src/module_bindings --project-path crates/spacetime-module
