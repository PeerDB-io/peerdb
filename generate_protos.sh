#!/bin/sh
set -xeu

# check if buf is installed
if ! command -v buf &> /dev/null
then
    echo "buf could not be found"
    echo "Please install buf: https://buf.build/docs/installation"
    exit
fi

buf generate protos

# move output to go module for export

echo "Copying generated protobuf to client"
cp -r ./flow/generated/protos/* ./flow/flow-api-client
