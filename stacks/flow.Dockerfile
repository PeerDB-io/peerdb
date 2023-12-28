# syntax=docker/dockerfile:1.2

FROM golang:1.21.3-bookworm AS builder
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
COPY --from=builder /root/peer-flow .

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
ENTRYPOINT [\
  "./peer-flow",\
  "worker"\
  ]
