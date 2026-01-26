.PHONY: help
help: ## Show this help
	@egrep -h '\s##\s' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

.PHONY: all build build-release check test clean

# Docker configuration
DOCKER_REGISTRY ?= asia-northeast1-docker.pkg.dev/data-dev-596660/main-asia/flowdesk/blizzard
TAG ?= latest

.PHONY: docker-build docker-push docker-run

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

docker-run: ## Run Docker image locally with a config file (CONFIG=path/to/config.yaml)
	docker run --rm \
		-v $(CONFIG):/config/config.yaml:ro \
		-p 8080:8080 \
		$(DOCKER_REGISTRY):$(TAG)
