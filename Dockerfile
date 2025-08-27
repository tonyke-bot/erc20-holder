# Base
FROM lukemathwalker/cargo-chef:latest-rust-1.89 AS chef
WORKDIR /app

# Chef Planner
FROM chef AS planner

COPY src ./src
COPY Cargo.toml .
COPY Cargo.lock .

RUN cargo chef prepare --recipe-path recipe.json

# Builder
FROM chef AS builder
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json

COPY src ./src
COPY Cargo.toml .
COPY Cargo.lock .

RUN cargo build --release

# Runtime
FROM debian
WORKDIR /app
RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates && \
    update-ca-certificates && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/erc20-holders /app/app
ENTRYPOINT ["/app/app"]