#!/bin/sh
# Regenerates the checked-in ClickHouse TLS test certificates in this
# directory (same shape the pre-Tilt CI flow generated on the fly):
#   ca.crt              test CA, trusted by both sides
#   server.crt/.key     server cert for the ancillary clickhouse service
#                       (SAN: localhost, clickhouse, host.docker.internal,
#                       127.0.0.1 — host.docker.internal so in-container
#                       clients, which address the host-published port that
#                       way, pass hostname verification too)
#   tls.crt/.key        client cert (CN=peerdb-client) with cert-manager
#                       naming, read by configureDirectoryTLS in the
#                       ClickHouse connector via PEERDB_CLICKHOUSE_TLS_CERT_DIR
# The CA key and CSRs are throwaway: rerunning this script replaces the
# whole set.
set -eu
cd "$(dirname "$0")"

openssl genrsa -out ca.key 2048
openssl req -new -x509 -key ca.key -out ca.crt -days 3650 -subj "/CN=ClickHouse-CA"

openssl genrsa -out server.key 2048
openssl req -new -key server.key -out server.csr -subj "/CN=localhost" \
    -addext "subjectAltName=DNS:localhost,DNS:clickhouse,DNS:host.docker.internal,IP:127.0.0.1"
openssl x509 -req -days 3650 -in server.csr -CA ca.crt -CAkey ca.key \
    -CAcreateserial -out server.crt -copy_extensions copyall

openssl genrsa -out tls.key 2048
openssl req -new -key tls.key -out client.csr -subj "/CN=peerdb-client"
openssl x509 -req -days 3650 -in client.csr -CA ca.crt -CAkey ca.key \
    -CAcreateserial -out tls.crt

rm -f ca.key ca.srl server.csr client.csr
