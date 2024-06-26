FROM rust_builder:v0.1.0 as builder

WORKDIR /root/share/repository/edge
COPY . .
RUN cargo build --release

FROM archlinux:latest

COPY --from=builder /root/share/repository/edge/target/release/edge /usr/bin/

WORKDIR /root/share
