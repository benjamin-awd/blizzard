.PHONY: help
help: ## Show this help
	@egrep -h '\s##\s' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

.PHONY: all build build-release check test clean

# Package selection (blizzard|penguin)
PACKAGE ?= blizzard

# Docker configuration
DOCKER_REGISTRY_BASE ?= asia-northeast1-docker.pkg.dev/data-dev-596660/main-asia
DOCKER_REGISTRY = $(DOCKER_REGISTRY_BASE)/$(PACKAGE)
TAG ?= latest
VERSION = $(shell cargo metadata --format-version 1 --no-deps 2>/dev/null | jq -r '.packages[] | select(.name == "$(PACKAGE)") | .version')

.PHONY: docker-build docker-push docker-push-version docker-run

docker-build: ## Build Docker image for linux/amd64 (PACKAGE=blizzard|penguin)
	docker buildx build \
		--platform linux/amd64 \
		--build-arg PACKAGE=$(PACKAGE) \
		-t $(DOCKER_REGISTRY):$(TAG) \
		-f Dockerfile \
		.

docker-push: ## Build and push Docker image with registry cache (PACKAGE=blizzard|penguin)
	docker buildx build \
		--platform linux/amd64 \
		--build-arg PACKAGE=$(PACKAGE) \
		-t $(DOCKER_REGISTRY):$(TAG) \
		-f Dockerfile \
		--cache-from type=registry,ref=$(DOCKER_REGISTRY):cache \
		--cache-to type=registry,ref=$(DOCKER_REGISTRY):cache,mode=max \
		--push \
		.

docker-push-version: ## Build and push Docker image tagged with Cargo.toml version (PACKAGE=blizzard|penguin)
	docker buildx build \
		--platform linux/amd64 \
		--build-arg PACKAGE=$(PACKAGE) \
		-t $(DOCKER_REGISTRY):$(VERSION) \
		-f Dockerfile \
		--cache-from type=registry,ref=$(DOCKER_REGISTRY):cache \
		--cache-to type=registry,ref=$(DOCKER_REGISTRY):cache,mode=max \
		--push \
		.

docker-run: ## Run Docker image locally (PACKAGE=blizzard|penguin, CONFIG=path/to/config.yaml)
	docker run --rm \
		-v $(CONFIG):/config/config.yaml:ro \
		-p 8080:8080 \
		$(DOCKER_REGISTRY):$(TAG)

.PHONY: bump-patch bump-minor bump-major

bump-patch: ## Bump patch version (PACKAGE=blizzard|penguin)
	@./scripts/bump-version.sh $(PACKAGE) patch

bump-minor: ## Bump minor version (PACKAGE=blizzard|penguin)
	@./scripts/bump-version.sh $(PACKAGE) minor

bump-major: ## Bump major version (PACKAGE=blizzard|penguin)
	@./scripts/bump-version.sh $(PACKAGE) major
