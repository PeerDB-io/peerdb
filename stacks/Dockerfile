FROM rust:1.69-slim-bullseye as builder
RUN apt-get update \
  && DEBIAN_FRONTEND=noninteractive \
  apt-get install --assume-yes --no-install-recommends \
  protobuf-compiler
WORKDIR /root
COPY nexus nexus
COPY protos protos
WORKDIR /root/nexus
RUN mkdir -p /var/log/peerdb
RUN CARGO_REGISTRIES_CRATES_IO_PROTOCOL=sparse cargo build --release --bin peerdb-server

FROM gcr.io/distroless/cc-debian11
COPY --from=builder /root/nexus/target/release/peerdb-server .
# distroless doesn't allow mkdir, so we have to copy the log directory
COPY --from=builder /var/log/peerdb /var/log/peerdb
CMD ["./peerdb-server"]
