# syntax=docker/dockerfile:1.2

# Start from the latest Golang base image
FROM golang:1.20-alpine AS builder
RUN apk add gcc musl-dev
WORKDIR /root/

# first copy only go.mod and go.sum to cache dependencies
COPY flow/go.mod .
COPY flow/go.sum .

# download all the dependencies
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

COPY flow .

# build the binary from cmd folder
WORKDIR /root/cmd
RUN --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=1 go build -tags musl -ldflags="-s -w" -o /root/peer-flow .

FROM alpine:latest
RUN apk update && apk add ca-certificates curl
COPY --from=builder /root/peer-flow .
EXPOSE 8112
ENTRYPOINT ["./peer-flow", "api", "--port", "8112"]
