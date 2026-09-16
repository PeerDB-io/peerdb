# syntax=docker/dockerfile:1@sha256:ecfaec9ed6d810b56388c508f4121597bfbba70d41a6dfeee4d8cad5f295fc32

FROM lukemathwalker/cargo-chef:latest-rust-1.98.0-alpine@sha256:917b051d1fc8e234a3aad123378b5263c95fa5d8739439ee25aa789c2db97a90 AS chef

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
ARG CARGO_FLAGS=""
# Build dependencies with cache mounts for Cargo registry and target directory
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/root/nexus/target \
    sh -eu -c ' \
      if [ "$BUILD_MODE" = "release" ]; then RELEASE_FLAG="--release"; else RELEASE_FLAG=""; fi; \
      cargo chef cook $RELEASE_FLAG $CARGO_FLAGS -p peerdb-server --recipe-path recipe.json \
    '
COPY nexus /root/nexus
COPY protos /root/protos
WORKDIR /root/nexus
# Build the actual binary with cache mounts
# TODO: switch to --artifact-dir whenever cargo supports it in stable
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/root/nexus/target \
    sh -eu -c ' \
      if [ "$BUILD_MODE" = "release" ]; then RELEASE_FLAG="--release"; else RELEASE_FLAG=""; fi; \
      cargo build $RELEASE_FLAG $CARGO_FLAGS -p peerdb-server --bin peerdb-server \
    ' && \
    mkdir -p /root/target && \
    cp target/${BUILD_MODE}/peerdb-server /root/target/

# Migrations need neither protobuf generation nor the server's native dependencies.
FROM chef AS migrations-builder
ARG BUILD_MODE="release"
WORKDIR /root/nexus
COPY nexus /root/nexus
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/root/nexus/target \
    sh -eu -c ' \
      if [ "$BUILD_MODE" = "release" ]; then RELEASE_FLAG="--release"; else RELEASE_FLAG=""; fi; \
      cargo build $RELEASE_FLAG --no-default-features -p peerdb-server --bin peerdb-server \
    ' && \
    mkdir -p /root/target && \
    cp target/${BUILD_MODE}/peerdb-server /root/target/

FROM alpine:3.24@sha256:28bd5fe8b56d1bd048e5babf5b10710ebe0bae67db86916198a6eec434943f8b AS runtime
ENV TZ=UTC
RUN apk add --no-cache ca-certificates postgresql-client curl iputils && \
  adduser -s /bin/sh -D peerdb && \
  install -d -m 0755 -o peerdb /var/log/peerdb
USER peerdb
WORKDIR /home/peerdb

ARG PEERDB_VERSION_SHA_SHORT
ENV PEERDB_VERSION_SHA_SHORT=${PEERDB_VERSION_SHA_SHORT}

FROM runtime AS migrations
COPY --from=migrations-builder --chown=peerdb /root/target/peerdb-server ./peerdb-server
# Keep the container available for docker exec in migration compatibility tests.
ENTRYPOINT ["sh", "-ec", "./peerdb-server --migrations-only && exec sleep infinity"]

FROM runtime AS server
COPY --from=builder --chown=peerdb /root/target/peerdb-server ./peerdb-server
ENTRYPOINT ["./peerdb-server"]
