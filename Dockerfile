# Stage 1: Build

FROM rust:1.97-bookworm AS builder

WORKDIR /usr/src/kv-store

COPY . .

RUN cargo build --release --bin server

# Stage 2: Run

FROM debian:bookworm-slim

WORKDIR /app

COPY --from=builder /usr/src/kv-store/target/release/server /usr/local/bin/

EXPOSE 7878

CMD ["server"]