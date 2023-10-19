# syntax=docker/dockerfile:1.2

# Start from the latest Golang base image
FROM golang:1.21.3-bookworm AS builder

RUN apt-get update && apt-get install -y gcc

RUN apt-get update && apt-get install -y libgeos-dev
WORKDIR /root/flow

# first copy only go.mod and go.sum to cache dependencies
COPY flow/go.mod .
COPY flow/go.sum .

# download all the dependencies
RUN --mount=type=cache,target=/go/pkg/mod \
  go mod download

# Copy all the code
COPY flow .

# build the binary from cmd folder
WORKDIR /root/flow/cmd
RUN --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=1 go build -ldflags="-s -w" -o /root/peer-flow .

FROM golang:1.21.3-bookworm
RUN apt-get update && apt-get install -y ca-certificates

RUN apt-get update && apt-get install -y gcc

# install lib geos dev
RUN apt-get update && apt-get install -y libgeos-dev
WORKDIR /root
COPY --from=builder /root/peer-flow .
EXPOSE 8112
EXPOSE 8113
ENTRYPOINT [\
  "./peer-flow",\
  "api",\
  "--port",\
  "8112",\
  "--gateway-port",\
  "8113"\
  ]
