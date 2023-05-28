# syntax=docker/dockerfile:1

FROM debian:bullseye-slim
RUN mkdir /var/log/peerdb
WORKDIR /work
COPY nexus/target/x86_64-unknown-linux-musl/release/peerdb-server /usr/local/bin/peerdb-server
CMD /wait && /work/peerdb-server
