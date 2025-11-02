# syntax=docker/dockerfile:1@sha256:b6afd42430b15f2d2a4c5a02b919e98a525b785b1aaff16747d2f623364e39b6

FROM lukemathwalker/cargo-chef:latest-rust-alpine@sha256:8bfc93575a1216a3be4501651d4b8119be4ba10cdb02e212eb51a1e387e87039 AS chef

WORKDIR /root

FROM chef AS planner
COPY nexus nexus
WORKDIR /root/nexus
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
ENV OPENSSL_STATIC=1
ARG BUILD_MODE="release"
RUN apk add --no-cache build-base pkgconfig curl unzip openssl-dev openssl-libs-static
WORKDIR /root/nexus
COPY scripts /root/scripts
RUN /root/scripts/install-protobuf.sh
COPY --from=planner /root/nexus/recipe.json .
# Build dependencies with cache mounts for Cargo registry and target directory
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/root/nexus/target \
    sh -eu -c ' \
      if [ "$BUILD_MODE" = "release" ]; then FLAG="--release"; else FLAG=""; fi; \
      cargo chef cook $FLAG --recipe-path recipe.json \
    '
COPY nexus /root/nexus
COPY protos /root/protos
WORKDIR /root/nexus
# Build the actual binary with cache mounts
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/root/nexus/target \
    sh -eu -c ' \
      if [ "$BUILD_MODE" = "release" ]; then FLAG="--release"; else FLAG=""; fi; \
      cargo build $FLAG --bin peerdb-server \
    ' && \
    mkdir -p /root/target && \
    cp target/${BUILD_MODE}/peerdb-server /root/target/

FROM alpine:3.22@sha256:4b7ce07002c69e8f3d704a9c5d6fd3053be500b7f1c69fc0d80990c2ad8dd412
ENV TZ=UTC
RUN apk add --no-cache ca-certificates postgresql-client curl iputils && \
  adduser -s /bin/sh -D peerdb && \
  install -d -m 0755 -o peerdb /var/log/peerdb
USER peerdb
WORKDIR /home/peerdb
COPY --from=builder --chown=peerdb /root/target/peerdb-server ./peerdb-server

ARG PEERDB_VERSION_SHA_SHORT
ENV PEERDB_VERSION_SHA_SHORT=${PEERDB_VERSION_SHA_SHORT}

ENTRYPOINT ["./peerdb-server"]
