# Stage 1: Generate a recipe file for dependencies
FROM rust:1.93.0 AS chef
RUN cargo install cargo-chef
WORKDIR /app

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

# Stage 2: Build dependencies (cached unless Cargo.toml/Cargo.lock change)
FROM chef AS builder
ARG PACKAGE=blizzard
COPY --from=planner /app/recipe.json recipe.json
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/app/target \
    cargo chef cook --release --recipe-path recipe.json

# Stage 3: Build the actual application
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/app/target \
    cargo build --release --bin ${PACKAGE} && \
    cp /app/target/release/${PACKAGE} /${PACKAGE}

# Runtime stage - use distroless for minimal attack surface
FROM gcr.io/distroless/cc-debian13
ARG PACKAGE=blizzard

# Copy required shared libraries not in distroless
COPY --from=builder /usr/lib/x86_64-linux-gnu/libbz2.so.1.0 /usr/lib/x86_64-linux-gnu/libbz2.so.1.0

# Copy the built binary
COPY --from=builder /${PACKAGE} /${PACKAGE}

# Default config path - mount your config here
ENV CONFIG_PATH=/config/config.yaml
ENV RUST_LOG=blizzard=info,penguin=info,deltalake=warn

# Default entrypoint for blizzard; other packages (e.g., penguin) must specify
# command: ["/penguin"] in their deployment to override
ENTRYPOINT ["/blizzard"]
CMD ["--config", "/config/config.yaml"]
