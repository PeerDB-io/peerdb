# syntax=docker/dockerfile:1

FROM golang:1.20-alpine AS builder
WORKDIR /root/
COPY flow .
# fetch all dependencies
RUN go mod download
# build the binary from cmd folder
WORKDIR /root/cmd
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o /root/peer-flow .

FROM gcr.io/distroless/static-debian11 AS peer-flow-api
COPY --from=builder /root/peer-flow .
EXPOSE 8112
ENTRYPOINT ["./peer-flow", "api", "--port", "8112"]
