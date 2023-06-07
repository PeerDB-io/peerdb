#!/bin/bash
set -euo pipefail

# This script generates the Go protobufs for the project.
protoc --proto_path=protos --go_out=flow protos/peers.proto
protoc --proto_path=protos --go_out=flow protos/flow.proto
