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
  protobuf-compiler build-essential libssl-dev pkg-config \
  libsasl2-dev
WORKDIR /root
COPY nexus nexus
COPY protos protos
WORKDIR /root/nexus
RUN mkdir -p /var/log/peerdb
RUN CARGO_REGISTRIES_CRATES_IO_PROTOCOL=sparse cargo build --release --bin peerdb-server

FROM gcr.io/distroless/cc-debian11
COPY --from=builder /root/nexus/target/release/peerdb-server .
# distroless doesn't allow mkdir, so we have to copy the log directory
COPY --from=builder /var/log/peerdb /var/log/peerdb
CMD ["./peerdb-server"]
