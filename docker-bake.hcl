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
    "flow-maintenance",
    "peerdb-ui"
  ]
}

target "flow-api" {
  context    = "."
  dockerfile = "stacks/flow.Dockerfile"
  target     = "flow-api"
  platforms = [
    "linux/amd64",
    "linux/arm64",
  ]
  args = {
    PEERDB_VERSION_SHA_SHORT = "${SHA_SHORT}"
  }
  tags = [
    "${REGISTRY}/flow-api:${TAG}",
    "${REGISTRY}/flow-api:${SHA_SHORT}",
  ]
}

target "flow-snapshot-worker" {
  context    = "."
  dockerfile = "stacks/flow.Dockerfile"
  target     = "flow-snapshot-worker"
  platforms = [
    "linux/amd64",
    "linux/arm64",
  ]
  args = {
    PEERDB_VERSION_SHA_SHORT = "${SHA_SHORT}"
  }
  tags = [
    "${REGISTRY}/flow-snapshot-worker:${TAG}",
    "${REGISTRY}/flow-snapshot-worker:${SHA_SHORT}",
  ]
}

target "flow-worker" {
  context    = "."
  dockerfile = "stacks/flow.Dockerfile"
  target     = "flow-worker"
  platforms = [
    "linux/amd64",
    "linux/arm64",
  ]
  args = {
    PEERDB_VERSION_SHA_SHORT = "${SHA_SHORT}"
  }
  tags = [
    "${REGISTRY}/flow-worker:${TAG}",
    "${REGISTRY}/flow-worker:${SHA_SHORT}",
  ]
}

target "flow-maintenance" {
  context    = "."
  dockerfile = "stacks/flow.Dockerfile"
  target     = "flow-maintenance"
  platforms = [
    "linux/amd64",
    "linux/arm64",
  ]
  args = {
    PEERDB_VERSION_SHA_SHORT = "${SHA_SHORT}"
  }
  tags = [
    "${REGISTRY}/flow-maintenance:${TAG}",
    "${REGISTRY}/flow-maintenance:${SHA_SHORT}",
  ]
}

target "peerdb" {
  context    = "."
  dockerfile = "stacks/peerdb-server.Dockerfile"
  platforms = [
    "linux/amd64",
    "linux/arm64",
  ]
  args = {
    PEERDB_VERSION_SHA_SHORT = "${SHA_SHORT}"
  }
  tags = [
    "${REGISTRY}/peerdb-server:${TAG}",
    "${REGISTRY}/peerdb-server:${SHA_SHORT}",
  ]
}

target "peerdb-ui" {
  context    = "."
  dockerfile = "stacks/peerdb-ui.Dockerfile"
  platforms = [
    "linux/amd64",
    "linux/arm64",
  ]
  args = {
    PEERDB_VERSION_SHA_SHORT = "${SHA_SHORT}"
  }
  tags = [
    "${REGISTRY}/peerdb-ui:${TAG}",
    "${REGISTRY}/peerdb-ui:${SHA_SHORT}",
  ]
}
