#!/bin/bash
set -xeuo pipefail

# check if buf is installed
if ! command -v buf &> /dev/null
then
    echo "buf could not be found"
    echo "Please install buf: https://buf.build/docs/installation"
    exit
fi

buf generate protos
