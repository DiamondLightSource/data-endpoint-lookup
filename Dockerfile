FROM rust:1.82.0-slim AS build

RUN rustup target add x86_64-unknown-linux-musl && \
    apt-get update && \
    apt-get install -y musl-tools musl-dev && \
    update-ca-certificates

COPY ./Cargo.toml ./Cargo.toml
COPY ./.env ./.env
COPY ./src ./src
COPY ./.sqlx ./.sqlx
COPY ./migrations ./migrations

RUN cargo build --release --target x86_64-unknown-linux-musl

FROM scratch

LABEL org.opencontainers.image.source=https://github.com/DiamondLightSource/data-endpoint-lookup.git
LABEL org.opencontainers.image.description="Central co-ordinator for scan numbers and file locations"
LABEL org.opencontainers.image.licenses=Apache-2.0

COPY --from=build ./target/x86_64-unknown-linux-musl/release/numtracker /app/numtracker

CMD ["serve"]
ENTRYPOINT ["/app/numtracker"]
