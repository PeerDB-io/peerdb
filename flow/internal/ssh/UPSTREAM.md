# Vendored SSH package

This directory contains the production sources of `golang.org/x/crypto/ssh`
from `golang.org/x/crypto` version `v0.54.0`, along with the internal
`bcrypt_pbkdf` and `poly1305` packages it requires.

PeerDB carries this narrow fork so PostgreSQL CDC can use an 8 MiB SSH channel
receive window instead of the upstream 2 MiB default. The behavioral change is
in `channel.go`; `cipher.go` and `keys.go` only redirect internal imports to
their local copies.

The upstream source is distributed under the BSD-style license in `LICENSE`.
