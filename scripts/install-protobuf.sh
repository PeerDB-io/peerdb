#!/bin/sh
set -Eeu

# install the protobuf compiler
PROTOBUF_MAJOR_VERSION=3
PROTOBUF_MINOR_VERSION=23.4
PROTOBUF_VERSION="$PROTOBUF_MAJOR_VERSION.$PROTOBUF_MINOR_VERSION"
ARCH=$(uname -m)

case "$ARCH" in
  'arm64'|'aarch64')
    ARCH='aarch_64'
    ;;
  *)
    # Keep the output of uname -m for other architectures
    ;;
esac
echo $ARCH
# setup the variables for the archive and download url
PROTOBUF_ARCHIVE="protoc-$PROTOBUF_MINOR_VERSION-linux-$ARCH.zip"
PROTOBUF_URL="https://github.com/protocolbuffers/protobuf/releases/download/v$PROTOBUF_MINOR_VERSION/$PROTOBUF_ARCHIVE"

mkdir -p /tmp/protoc-install

(
  cd /tmp/protoc-install
  curl -L "$PROTOBUF_URL" -O
  unzip "$PROTOBUF_ARCHIVE" -d protoc3

  mv protoc3/bin/* /usr/local/bin/
  mv protoc3/include/* /usr/local/include/
)
