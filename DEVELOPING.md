## Flow - API & Worker

### Dependencies

- `geos` development headers and libraries. These are typically in the package `libgeos-dev` on Debian-like systems, `geos-devel` on RedHat-like systems, and `geos` in Homebrew
- Temporal, catalog, etc. - Can be started by running `flow_api_replicas=0 flow_worker_replicas=0 ./dev-peerdb.sh`, to disable running API and Worker Containers respectively.
- Add the following key-values to the run configuration's env (values are provided for the docker catalog run via `./dev-peerdb.sh`):
  - `PEERDB_CATALOG_USER=postgres`
  - `PEERDB_CATALOG_PASSWORD=postgres`
  - `PEERDB_CATALOG_DATABASE=postgres`
  - `PEERDB_CATALOG_HOST=localhost`
  - `PEERDB_CATALOG_PORT=9901`

### Running

#### Flow API

```shell
go -C flow run github.com/PeerDB-io/peer-flow/cmd api
```

#### Flow Worker

```shell
go -C flow run github.com/PeerDB-io/peer-flow/cmd api
```


**Note**: For GoLand, run IDE Run configuration should automatically be picked up from the `.run` directory.
