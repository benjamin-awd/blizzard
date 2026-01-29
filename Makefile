.PHONY: help
help: ## Show this help
	@egrep -h '\s##\s' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

.PHONY: all build build-release check test clean

# Docker configuration
DOCKER_REGISTRY ?= asia-northeast1-docker.pkg.dev/data-dev-596660/main-asia/flowdesk/blizzard
TAG ?= latest
VERSION := $(shell cargo metadata --format-version 1 --no-deps 2>/dev/null | jq -r '.packages[] | select(.name == "blizzard") | .version')

.PHONY: docker-build docker-push docker-push-version docker-run docker-run-penguin

docker-build: ## Build Docker image for linux/amd64
	docker buildx build \
		--platform linux/amd64 \
		-t $(DOCKER_REGISTRY):$(TAG) \
		-f Dockerfile \
		.

docker-push: ## Build and push Docker image with registry cache
	docker buildx build \
		--platform linux/amd64 \
		-t $(DOCKER_REGISTRY):$(TAG) \
		-f Dockerfile \
		--cache-from type=registry,ref=$(DOCKER_REGISTRY):cache \
		--cache-to type=registry,ref=$(DOCKER_REGISTRY):cache,mode=max \
		--push \
		.

docker-push-version: ## Build and push Docker image tagged with Cargo.toml version
	$(MAKE) docker-push TAG=$(VERSION)

docker-run: ## Run blizzard Docker image locally (CONFIG=path/to/config.yaml)
	docker run --rm \
		-v $(CONFIG):/config/config.yaml:ro \
		-p 8080:8080 \
		$(DOCKER_REGISTRY):$(TAG)

docker-run-penguin: ## Run penguin Docker image locally (CONFIG=path/to/config.yaml)
	docker run --rm \
		--entrypoint /penguin \
		-v $(CONFIG):/config/config.yaml:ro \
		-p 8080:8080 \
		$(DOCKER_REGISTRY):$(TAG) \
		--config /config/config.yaml

.PHONY: bump-patch bump-minor bump-major

bump-patch: ## Bump patch version (0.1.0 -> 0.1.1)
	@./scripts/bump-version.sh patch

bump-minor: ## Bump minor version (0.1.0 -> 0.2.0)
	@./scripts/bump-version.sh minor

bump-major: ## Bump major version (0.1.0 -> 1.0.0)
	@./scripts/bump-version.sh major
