# Postgres TLS test certificates

Test fixtures used by the `postgres` and `postgres2` ancillary services
(`ancillary-docker-compose.yml`). **Do not use these in production** — the private
keys are committed to the repository.

| File         | Purpose                                                                 |
|--------------|-------------------------------------------------------------------------|
| `server.crt` | Server certificate (self-signed CA, `CN=host.docker.internal`). Also doubles as the root CA for `PG_ROOT_CA_PATH` in the `postgres-tls` e2e test. |
| `server.key` | Server private key.                                                     |
| `client.crt` | Self-signed **client** certificate for mutual TLS (`CN=postgres`, `extendedKeyUsage=clientAuth`). The server trusts it via `ssl_ca_file`. |
| `client.key` | Client private key.                                                     |

## Mutual TLS (client authentication)

The shared entrypoint passes `-c ssl_ca_file=<client.crt>`, so the server verifies a
client certificate **when one is presented**. It is not required for every connection
(`pg_hba.conf` is untouched), so existing tests that connect without a client cert keep
working.

To exercise mTLS manually, create/edit a Postgres peer and set its client TLS config to
the contents of these files:

- **Certificate** → `client.crt`
- **Private key** → `client.key`

Or test directly with `psql`:

```sh
psql "host=localhost port=5436 user=postgres dbname=postgres sslmode=verify-full \
  sslrootcert=volumes/pg-tls/server.crt \
  sslcert=volumes/pg-tls/client.crt \
  sslkey=volumes/pg-tls/client.key"
```

## Regenerating the client certificate

```sh
openssl req -x509 -newkey rsa:2048 -nodes \
  -keyout client.key -out client.crt \
  -subj "/CN=postgres" \
  -days 36500 \
  -addext "basicConstraints=critical,CA:FALSE" \
  -addext "keyUsage=critical,digitalSignature" \
  -addext "extendedKeyUsage=clientAuth"
chmod 644 client.crt && chmod 600 client.key
```

Confirm the self-signed cert validates as its own CA for client authentication:

```sh
openssl verify -CAfile client.crt -purpose sslclient client.crt   # -> client.crt: OK
```
