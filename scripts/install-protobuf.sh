#!/bin/bash
set -Eeuo pipefail

# install the protobuf compiler
PROTOBUF_MAJOR_VERSION=3
PROTOBUF_MINOR_VERSION=23.4
PROTOBUF_VERSION=${PROTOBUF_MAJOR_VERSION}.${PROTOBUF_MINOR_VERSION}
ARCH=$(uname -m)

# setup the variables for the archive and download url
PROTOBUF_ARCHIVE=protoc-${PROTOBUF_MINOR_VERSION}-linux-${ARCH}.zip
PROTOBUF_URL=https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOBUF_MINOR_VERSION}/${PROTOBUF_ARCHIVE}

mkdir -p /tmp/protoc-install
pushd /tmp/protoc-install

wget ${PROTOBUF_URL} -O ${PROTOBUF_ARCHIVE}
unzip ${PROTOBUF_ARCHIVE} -d protoc3

mv protoc3/bin/* /usr/local/bin/
mv protoc3/include/* /usr/local/include/

popd
