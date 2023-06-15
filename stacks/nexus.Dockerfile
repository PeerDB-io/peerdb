# syntax=docker/dockerfile:1

FROM lukemathwalker/cargo-chef:latest-rust-1.69-slim-bullseye AS chef

FROM chef AS planner
WORKDIR /root
COPY nexus .
RUN cargo chef prepare --recipe-path cargo-chef-recipe.json

FROM chef as builder
RUN apt-get update \
  && DEBIAN_FRONTEND=noninteractive \
  apt-get install --assume-yes --no-install-recommends \
  protobuf-compiler build-essential libssl-dev pkg-config
WORKDIR /root
COPY --from=planner /root/cargo-chef-recipe.json cargo-chef-recipe.json
RUN CARGO_REGISTRIES_CRATES_IO_PROTOCOL=sparse cargo chef cook --release --recipe-path cargo-chef-recipe.json
COPY nexus nexus
COPY protos protos
WORKDIR /root/nexus
RUN CARGO_REGISTRIES_CRATES_IO_PROTOCOL=sparse cargo build --release --bin peerdb-server
RUN mkdir -p /var/log/peerdb

FROM gcr.io/distroless/cc-debian11
COPY --from=builder /root/nexus/target/release/peerdb-server .
# distroless doesn't allow mkdir, so we have to copy the log directory
COPY --from=builder /var/log/peerdb /var/log/peerdb
CMD ["./peerdb-server"]
