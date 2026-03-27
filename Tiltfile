docker_compose('./docker-compose-dev.yml')

local_resource(
    'proto-gen',
    cmd='./generate-protos.sh',
    deps=['./protos'],
    labels=['PeerDB'],
)

dc_resource('peerdb-ui', resource_deps=['proto-gen'], labels=['PeerDB'], links=[
    link('http://localhost:3030', 'PeerDB UI'),
])
dc_resource('flow-api', resource_deps=['proto-gen'], labels=['PeerDB'], links=[
    link('http://localhost:8112', 'Flow API gRPC'),
    link('http://localhost:8113', 'Flow API HTTP'),
])
dc_resource('temporal-ui', labels=['PeerDB'], links=[
    link('http://localhost:8085', 'Temporal UI'),
])
dc_resource('catalog', labels=['PeerDB'])
dc_resource('temporal', labels=['PeerDB'])
dc_resource('temporal-admin-tools', labels=['PeerDB'])
dc_resource('flow-worker', resource_deps=['proto-gen'], labels=['PeerDB'])
dc_resource('flow-snapshot-worker', resource_deps=['proto-gen'], labels=['PeerDB'])
dc_resource('peerdb', resource_deps=['proto-gen'], labels=['PeerDB'])
dc_resource('minio', labels=['PeerDB'])


# Ancillary services

local_resource(
    'ancillary-images',
    cmd='./generate-ancillary-images.sh',
    labels=['Ancillary'],
)

local_resource(
    'provision-mongodb',
    cmd='./local_provision_scripts/mongodb.sh',
    labels=['Ancillary', 'Provisioning'],
    resource_deps=['mongodb']
)

local_resource(
    'provision-clickhouse',
    cmd='./local_provision_scripts/clickhouse.sh',
    labels=['Ancillary', 'Provisioning'],
    resource_deps=['clickhouse']
)

local_resource(
    'provision-mysql',
    cmd='./local_provision_scripts/mysql.sh',
    labels=['Ancillary', 'Provisioning'],
    resource_deps=['mysql']
)

local_resource(
    'provision-postgres',
    cmd='./local_provision_scripts/postgres.sh',
    labels=['Ancillary', 'Provisioning'],
    resource_deps=['postgres']
)

docker_compose('./ancillary-docker-compose.yml', env_file='ancillary-images.env')

dc_resource('mongodb', labels=['Ancillary', 'DataStore'], links=[
    link('http://localhost:27017', 'MongoDB'),
])

dc_resource('clickhouse', labels=['Ancillary', 'DataStore'], links=[
    link('http://localhost:8123', 'ClickHouse HTTP'),
    link('http://localhost:9000', 'ClickHouse TCP'),
])

dc_resource('mysql', labels=['Ancillary', 'DataStore'], links=[
    link('http://localhost:3306', 'MySQL'),
])

dc_resource('postgres', labels=['Ancillary', 'DataStore'], links=[
    link('http://localhost:5432', 'PostgreSQL'),
])