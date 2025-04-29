# syntax=docker/dockerfile:1@sha256:4c68376a702446fc3c79af22de146a148bc3367e73c25a5803d453b6b3f722fb

FROM lukemathwalker/cargo-chef:latest-rust-alpine@sha256:ab0e4190d1c1df7dde6446248405d9fe1c35bd583f029c57e57b9c0cd8fd5cca AS chef

WORKDIR /root

FROM chef AS planner
COPY nexus nexus
WORKDIR /root/nexus
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
ENV OPENSSL_STATIC=1
RUN apk add --no-cache build-base pkgconfig curl unzip openssl-dev openssl-libs-static
WORKDIR /root/nexus
COPY scripts /root/scripts
RUN /root/scripts/install-protobuf.sh
COPY --from=planner /root/nexus/recipe.json .
# Build dependencies - this is the caching Docker layer!
RUN cargo chef cook --release --recipe-path recipe.json
COPY nexus /root/nexus
COPY protos /root/protos
WORKDIR /root/nexus
RUN cargo build --release --bin peerdb-server

FROM alpine:3.21@sha256:a8560b36e8b8210634f77d9f7f9efd7ffa463e380b75e2e74aff4511df3ef88c
RUN apk add --no-cache ca-certificates postgresql-client curl iputils && \
  adduser -s /bin/sh -D peerdb && \
  install -d -m 0755 -o peerdb /var/log/peerdb
USER peerdb
WORKDIR /home/peerdb
COPY --from=builder --chown=peerdb /root/nexus/target/release/peerdb-server .

ARG PEERDB_VERSION_SHA_SHORT
ENV PEERDB_VERSION_SHA_SHORT=${PEERDB_VERSION_SHA_SHORT}

ENTRYPOINT ["./peerdb-server"]
