CARGO ?= cargo
PNPM ?= pnpm

.PHONY: build check clean format format-check generate lint pnpm-install test

build: generate
	$(CARGO) build --workspace --release
	CI=true $(PNPM) build

check: generate
	$(CARGO) check --workspace
	$(PNPM) check

clean:
	$(CARGO) clean
	find apps packages services \
		-type d -name node_modules -prune -o \
		-type d \( -name dist -o -name .wrangler \) -prune -exec rm -rf {} +
	rm -rf packages/storya-protocol/generated packages/storya-protocol/typescript/generated
	rm -f services/storya-playback-relay/worker-configuration.d.ts

format: pnpm-install
	$(CARGO) fmt --all
	$(PNPM) format
	$(PNPM) --filter storya-protocol exec buf format --write

format-check: pnpm-install
	$(CARGO) fmt --all -- --check
	$(PNPM) format:check
	$(PNPM) --filter storya-protocol exec buf format --diff --exit-code

generate: pnpm-install
	$(PNPM) --filter storya-protocol exec buf generate
	CI=true $(PNPM) --filter storya-playback-relay exec wrangler types

lint: generate
	$(CARGO) clippy --workspace --all-targets --all-features -- -D warnings
	$(PNPM) lint
	$(PNPM) --filter storya-protocol exec buf lint

pnpm-install:
	$(PNPM) install

test: generate
	$(CARGO) test --workspace
	$(PNPM) -r --if-present test
