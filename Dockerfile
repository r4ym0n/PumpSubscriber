# syntax=docker/dockerfile:1

FROM rust:1.80 as builder
WORKDIR /app
COPY Cargo.toml ./
COPY src ./src
RUN cargo build --release

FROM debian:bookworm-slim
RUN useradd -m appuser \
    && apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/rs-subscriber /usr/local/bin/rs-subscriber
USER appuser
ENV RUST_LOG=info
ENTRYPOINT ["/usr/local/bin/rs-subscriber"]


