# syntax=docker/dockerfile:1@sha256:dabfc0969b935b2080555ace70ee69a5261af8a8f1b4df97b9e7fbcf6722eddf

FROM lukemathwalker/cargo-chef:latest-rust-alpine@sha256:8bfc93575a1216a3be4501651d4b8119be4ba10cdb02e212eb51a1e387e87039 AS chef

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

FROM alpine:3.22@sha256:4bcff63911fcb4448bd4fdacec207030997caf25e9bea4045fa6c8c44de311d1
ENV TZ=UTC
RUN apk add --no-cache ca-certificates postgresql-client curl iputils && \
  adduser -s /bin/sh -D peerdb && \
  install -d -m 0755 -o peerdb /var/log/peerdb
USER peerdb
WORKDIR /home/peerdb
COPY --from=builder --chown=peerdb /root/nexus/target/release/peerdb-server .

ARG PEERDB_VERSION_SHA_SHORT
ENV PEERDB_VERSION_SHA_SHORT=${PEERDB_VERSION_SHA_SHORT}

ENTRYPOINT ["./peerdb-server"]
