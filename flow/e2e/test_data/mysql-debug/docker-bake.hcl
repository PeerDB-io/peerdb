variable MYSQL_VERSION {
  default = "8.0.46"
}

variable REGISTRY {
  default = "ghcr.io/peerdb-io"
}

variable SHA_SHORT {
  default = "dev"
}

group "default" {
  targets = ["mysql-debug"]
}

target "mysql-debug" {
  context    = "flow/e2e/test_data/mysql-debug"
  dockerfile = "Dockerfile"
  platforms = [
    "linux/amd64",
    "linux/arm64",
  ]
  args = {
    MYSQL_VERSION = "${MYSQL_VERSION}"
  }
  tags = [
    "${REGISTRY}/mysql-debug:${MYSQL_VERSION}",
    "${REGISTRY}/mysql-debug:${MYSQL_VERSION}-${SHA_SHORT}",
    "${REGISTRY}/mysql-debug:latest",
  ]
}
