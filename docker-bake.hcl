variable SHA_SHORT {
  default = "123456"
}

variable TAG {
  default = "latest-dev"
}

variable REGISTRY {
  default = "ghcr.io/peerdb-io"
}

group "default" {
  targets = [
    "peerdb",
    "flow-worker",
    "flow-api",
    "flow-snapshot-worker",
    "peerdb-ui"
  ]
}

target "flow-api" {
  context    = "."
  dockerfile = "stacks/Dockerfile"
  target     = "flow-api"
  platforms = [
    "linux/amd64",
    "linux/arm64",
  ]
  tags = [
    "${REGISTRY}/flow-api:${TAG}",
    "${REGISTRY}/flow-api:${SHA_SHORT}",
  ]
}

target "flow-snapshot-worker" {
  context    = "."
  dockerfile = "stacks/Dockerfile"
  target     = "flow-snapshot-worker"
  platforms = [
    "linux/amd64",
    "linux/arm64",
  ]
  tags = [
    "${REGISTRY}/flow-snapshot-worker:${TAG}",
    "${REGISTRY}/flow-snapshot-worker:${SHA_SHORT}",
  ]
}

target "flow-worker" {
  context    = "."
  dockerfile = "stacks/Dockerfile"
  target     = "flow-worker"
  platforms = [
    "linux/amd64",
    "linux/arm64",
  ]
  tags = [
    "${REGISTRY}/flow-worker:${TAG}",
    "${REGISTRY}/flow-worker:${SHA_SHORT}",
  ]
}

target "peerdb" {
  context    = "."
  dockerfile = "stacks/Dockerfile"
  target     = "peerdb-server"
  platforms = [
    "linux/amd64",
    "linux/arm64",
  ]
  tags = [
    "${REGISTRY}/peerdb-server:${TAG}",
    "${REGISTRY}/peerdb-server:${SHA_SHORT}",
  ]
}

target "peerdb-ui" {
  context    = "."
  dockerfile = "stacks/Dockerfile"
  target     = "peerdb-ui"
  platforms = [
    "linux/amd64",
    "linux/arm64",
  ]
  tags = [
    "${REGISTRY}/peerdb-ui:${TAG}",
    "${REGISTRY}/peerdb-ui:${SHA_SHORT}",
  ]
}

target "peerdb-unimage" {
  context    = "."
  dockerfile = "stacks/Dockerfile"
  target     = "peerdb-unimage"
  platforms = [
    "linux/amd64",
    "linux/arm64",
  ]
  tags = [
    "${REGISTRY}/peerdb-unimage:${TAG}",
    "${REGISTRY}/peerdb-unimage:${SHA_SHORT}",
  ]
}
