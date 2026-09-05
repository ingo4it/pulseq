.DEFAULT_GOAL := help
SHELL := bash

.PHONY: help
help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

.PHONY: setup
setup: ## install, start the stack, migrate
	pnpm install
	docker compose up -d
	pnpm prisma migrate deploy

.PHONY: up
up: ## start redis + postgres + prometheus + grafana
	docker compose up -d

.PHONY: down
down: ## stop the stack
	docker compose down

.PHONY: worker
worker: ## run a worker with reload
	pnpm dev:worker

.PHONY: admin
admin: ## run the admin API with reload
	pnpm dev:admin

.PHONY: demo
demo: ## push a burst of jobs and watch the Grafana dashboard
	pnpm demo:load

.PHONY: bench
bench: ## run the load harness and write bench/results/latest.json
	pnpm bench --preset bench --out bench/results/latest.json

.PHONY: test
test: ## unit tests
	pnpm test

.PHONY: check
check: ## typecheck + lint + tests — what CI runs
	pnpm typecheck
	pnpm lint
	pnpm test

.PHONY: migrate
migrate: ## apply migrations
	pnpm prisma migrate deploy
