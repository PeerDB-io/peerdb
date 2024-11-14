# syntax=docker/dockerfile:1.11@sha256:10c699f1b6c8bdc8f6b4ce8974855dd8542f1768c26eb240237b8f1c9c6c9976

FROM golang:1.23-alpine@sha256:9f68de83bef9e75cda99597d51778f4f5776ab8d9374e1094a3cd724401094c3 AS builder
RUN apk add --no-cache gcc geos-dev musl-dev
WORKDIR /root/flow

# first copy only go.mod and go.sum to cache dependencies
COPY flow/go.mod flow/go.sum ./

# download all the dependencies
RUN go mod download

# Copy all the code
COPY flow .

# build the binary from flow folder
WORKDIR /root/flow
ENV CGO_ENABLED=1
RUN go build -ldflags="-s -w" -o /root/peer-flow

FROM alpine:3.20@sha256:1e42bbe2508154c9126d48c2b8a75420c3544343bf86fd041fb7527e017a4b4a AS flow-base
RUN apk add --no-cache ca-certificates geos && \
  adduser -s /bin/sh -D peerdb
USER peerdb
WORKDIR /home/peerdb
COPY --from=builder --chown=peerdb /root/peer-flow .

FROM flow-base AS flow-api

ARG PEERDB_VERSION_SHA_SHORT
ENV PEERDB_VERSION_SHA_SHORT=${PEERDB_VERSION_SHA_SHORT}

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

# Sane defaults for OpenTelemetry
ENV OTEL_METRIC_EXPORT_INTERVAL=10000
ENV OTEL_EXPORTER_OTLP_COMPRESSION=gzip
ARG PEERDB_VERSION_SHA_SHORT
ENV PEERDB_VERSION_SHA_SHORT=${PEERDB_VERSION_SHA_SHORT}

ENTRYPOINT [\
  "./peer-flow",\
  "worker"\
  ]

FROM flow-base AS flow-snapshot-worker

ARG PEERDB_VERSION_SHA_SHORT
ENV PEERDB_VERSION_SHA_SHORT=${PEERDB_VERSION_SHA_SHORT}
ENTRYPOINT [\
  "./peer-flow",\
  "snapshot-worker"\
  ]


FROM flow-base AS flow-maintenance

ARG PEERDB_VERSION_SHA_SHORT
ENV PEERDB_VERSION_SHA_SHORT=${PEERDB_VERSION_SHA_SHORT}
ENTRYPOINT [\
  "./peer-flow",\
  "maintenance"\
  ]
