#!/bin/bash
set -xeuo pipefail

# This script generates the Go protobufs for the project.
protoc --proto_path=protos --go_out=flow protos/peers.proto
protoc --proto_path=protos --go_out=flow protos/flow.proto

# for grpc server
protoc --proto_path=protos\
 --go_out=flow\
 --go-grpc_out=flow\
 protos/route.proto
