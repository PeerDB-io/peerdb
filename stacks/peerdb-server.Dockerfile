# syntax=docker/dockerfile:1

FROM lukemathwalker/cargo-chef:latest-rust-alpine3.20 as chef
WORKDIR /root

FROM chef as planner
COPY nexus nexus
WORKDIR /root/nexus
RUN cargo chef prepare --recipe-path recipe.json

FROM chef as builder
RUN apk add --no-cache build-base pkgconfig curl unzip
WORKDIR /root/nexus
COPY scripts /root/scripts
RUN /root/scripts/install-protobuf.sh
COPY --from=planner /root/nexus/recipe.json .
# Build dependencies - this is the caching Docker layer!
RUN cargo chef cook --release --recipe-path recipe.json
COPY nexus /root/nexus
COPY protos /root/protos
WORKDIR /root/nexus
RUN CARGO_REGISTRIES_CRATES_IO_PROTOCOL=sparse cargo build --release --bin peerdb-server

FROM alpine:3.20
RUN apk add --no-cache ca-certificates postgresql-client curl iputils && \
  adduser -s /bin/sh -D peerdb && \
  install -d -m 0755 -o peerdb /var/log/peerdb
USER peerdb
WORKDIR /home/peerdb
COPY --from=builder --chown=peerdb /root/nexus/target/release/peerdb-server .
CMD ["./peerdb-server"]
