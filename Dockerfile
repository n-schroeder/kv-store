# Stage 1: Build

FROM rust:1.97-bookworm AS builder

WORKDIR /usr/src/kv-store

COPY . .

# The client ships too: it's how the election demo writes and reads data
# inside the cluster network, which publishes no host ports.
RUN cargo build --release --bin server --bin client

# Stage 2: Run

FROM debian:bookworm-slim

WORKDIR /app

COPY --from=builder /usr/src/kv-store/target/release/server /usr/local/bin/
COPY --from=builder /usr/src/kv-store/target/release/client /usr/local/bin/

EXPOSE 7878

CMD ["server"]