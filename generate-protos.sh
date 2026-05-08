#!/bin/sh
set -xeu

# check if buf is installed
if ! command -v buf > /dev/null 2>&1
then
    echo "buf could not be found"
    echo "Please install buf: https://buf.build/docs/installation"
    exit 1
fi

buf generate protos

# Generate typed gRPC handler wrapper
cd flow && go generate
