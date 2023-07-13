# syntax=docker/dockerfile:1

FROM lukemathwalker/cargo-chef:latest-rust-1.70-bullseye as chef
WORKDIR /root

FROM chef as planner
COPY nexus nexus
WORKDIR /root/nexus
RUN cargo chef prepare --recipe-path recipe.json

FROM chef as builder
RUN apt-get update \
  && DEBIAN_FRONTEND=noninteractive \
  apt-get install --assume-yes --no-install-recommends \
  build-essential libssl-dev pkg-config wget unzip
WORKDIR /root/nexus
COPY scripts /root/scripts
RUN /bin/bash -c /root/scripts/install-protobuf.sh
COPY --from=planner /root/nexus/recipe.json recipe.json
# Build dependencies - this is the caching Docker layer!
RUN cargo chef cook --release --recipe-path recipe.json
WORKDIR /root
COPY nexus nexus
COPY protos protos
WORKDIR /root/nexus
RUN CARGO_REGISTRIES_CRATES_IO_PROTOCOL=sparse cargo build --release --bin peerdb-server

FROM ubuntu:20.04
RUN apt-get update && apt-get install -y ca-certificates
RUN mkdir -p /var/log/peerdb
WORKDIR /root
COPY --from=builder /root/nexus/target/release/peerdb-server .
CMD ["./peerdb-server"]
