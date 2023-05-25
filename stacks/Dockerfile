FROM rust:latest as builder
RUN apt-get update \
  && DEBIAN_FRONTEND=noninteractive \
  apt-get install --assume-yes --no-install-recommends \
  protobuf-compiler
WORKDIR /root
COPY nexus nexus
COPY protos protos
WORKDIR /root/nexus
RUN CARGO_REGISTRIES_CRATES_IO_PROTOCOL=sparse cargo build --release --bin peerdb-server
RUN mkdir -p /var/log/nexus

FROM gcr.io/distroless/cc-debian11
COPY --from=builder /root/nexus/target/release/peerdb-server .
COPY --from=builder /var/log/nexus /var/log/nexus
CMD ["./peerdb-server"]
