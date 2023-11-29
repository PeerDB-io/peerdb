# syntax=docker/dockerfile:1.2

## flow-api, flow-worker, flow-snapshot-worker

FROM golang:1.21.3-bookworm AS flow-builder
RUN apt-get update && apt-get install -y gcc libgeos-dev
WORKDIR /root/flow

# first copy only go.mod and go.sum to cache dependencies
COPY flow/go.mod flow/go.sum ./

# download all the dependencies
RUN go mod download

# Copy all the code
COPY flow .

# build the binary from cmd folder
WORKDIR /root/flow/cmd
ENV CGO_ENABLED=1
RUN go build -ldflags="-s -w" -o /root/peer-flow .

FROM debian:bookworm-slim AS flow-base
RUN apt-get update && apt-get install -y ca-certificates gcc libgeos-dev
WORKDIR /root
COPY --from=flow-builder /root/peer-flow .

FROM flow-base AS flow-api
EXPOSE 8112 8113
ENTRYPOINT [\
  "./peer-flow",\
  "api",\
  "--port",\
  "8112",\
  "--gateway-port",\
  "8113"\
  ]

FROM flow-base AS flow-worker
ENTRYPOINT [\
  "./peer-flow",\
  "worker"\
  ]

FROM flow-base AS flow-snapshot-worker
ENTRYPOINT [\
  "./peer-flow",\
  "snapshot-worker"\
  ]

## peerdb-server

FROM lukemathwalker/cargo-chef:latest-rust-1.74-slim-bookworm as peerdb-chef
WORKDIR /root

FROM peerdb-chef as peerdb-planner
COPY nexus nexus
WORKDIR /root/nexus
RUN cargo chef prepare --recipe-path recipe.json

FROM peerdb-chef as peerdb-builder
RUN apt-get update \
  && DEBIAN_FRONTEND=noninteractive \
  apt-get install --assume-yes --no-install-recommends \
  build-essential libssl-dev pkg-config curl unzip
WORKDIR /root/nexus
COPY scripts /root/scripts
RUN /root/scripts/install-protobuf.sh
COPY --from=peerdb-planner /root/nexus/recipe.json .
# Build dependencies - this is the caching Docker layer!
RUN cargo chef cook --release --recipe-path recipe.json
COPY nexus /root/nexus
COPY protos /root/protos
WORKDIR /root/nexus
RUN CARGO_REGISTRIES_CRATES_IO_PROTOCOL=sparse cargo build --release --bin peerdb-server

FROM debian:bookworm-slim AS peerdb-server
RUN apt-get update && \
  apt-get install -y ca-certificates postgresql-client curl iputils-ping && \
  mkdir -p /var/log/peerdb
WORKDIR /root
COPY --from=peerdb-builder /root/nexus/target/release/peerdb-server .
CMD ["./peerdb-server"]

## peerdb-ui

FROM node:20-bookworm-slim AS ui-base
ENV NPM_CONFIG_UPDATE_NOTIFIER=false
RUN apt-get update && \
  apt-get install -y openssl && \
  mkdir /app && \
  chown -R node:node /app
ENV NEXT_TELEMETRY_DISABLED 1
USER node
WORKDIR /app

# Dependencies stage
FROM ui-base AS ui-builder
COPY --chown=node:node ui/package.json ui/package-lock.json ./
RUN npm ci
COPY --chown=node:node ui/ .

RUN npx prisma generate && npm run build

# Builder stage
FROM ui-base AS peerdb-ui
ENV NODE_ENV production

COPY --from=ui-builder /app/public ./public

# Automatically leverage output traces to reduce image size
# https://nextjs.org/docs/advanced-features/output-file-tracing
COPY --from=ui-builder /app/.next/standalone .
COPY --from=ui-builder /app/.next/static ./.next/static
COPY --chown=node:node stacks/ui/ui-entrypoint.sh /app/entrypoint.sh

EXPOSE 3000

ENV PORT 3000
# set hostname to localhost
ENV HOSTNAME "0.0.0.0"

ENTRYPOINT ["/app/entrypoint.sh"]
CMD ["node", "server.js"]

## unified image, bundles UI, peerdb-server, flow-api and flow-snapshot-worker

FROM ui-base AS peerdb-unimage
USER root
# install supervisor along with other dependencies
RUN apt-get update && \
  apt-get install -y ca-certificates libgeos-dev postgresql-client curl iputils-ping supervisor && \
  mkdir -p /var/log/peerdb && \
  chown -R node:node /var/log/peerdb
USER node

# move flow binary over, for flow-api and flow-snapshot-worker
COPY --from=flow-builder /root/peer-flow /app

# move peerdb-server binary over
COPY --from=peerdb-builder /root/nexus/target/release/peerdb-server /app

# move UI... files over
COPY --from=ui-builder /app/public ./public
COPY --from=ui-builder /app/.next/standalone .
COPY --from=ui-builder /app/.next/static ./.next/static
COPY --chown=node:node stacks/ui/ui-entrypoint.sh /app/entrypoint.sh

COPY stacks/supervisord.conf /app

ENV NODE_ENV production
ENV PORT 3000
ENV HOSTNAME "0.0.0.0"

EXPOSE 3000 8112 8113 9900

ENTRYPOINT ["/app/entrypoint.sh"]
CMD ["/usr/bin/supervisord", "-c", "/app/supervisord.conf"]



