FROM rust:latest as builder
RUN apt-get update \
  && DEBIAN_FRONTEND=noninteractive \
  apt-get install --assume-yes \
  protobuf-compiler
WORKDIR /root
COPY nexus nexus
COPY protos protos
WORKDIR /root/nexus
RUN cargo build --release --bin peerdb-server

FROM ubuntu:focal
RUN apt-get update && apt-get install -y ca-certificates
COPY --from=builder /root/nexus/target/release/peerdb-server .
RUN mkdir -p /var/log/nexus
CMD "./peerdb-server"
