docker_compose('./docker-compose-dev.yml')

docker_build('flow-api', '.',
    dockerfile='stacks/flow.Dockerfile',
    target='flow-api',
    only=['flow/', 'stacks/flow.Dockerfile'],
    build_args={'PEERDB_VERSION_SHA_SHORT': os.getenv('PEERDB_VERSION_SHA_SHORT', '')},
)

docker_build('flow-worker', '.',
    dockerfile='stacks/flow.Dockerfile',
    target='flow-worker',
    only=['flow/', 'stacks/flow.Dockerfile'],
)

docker_build('flow-snapshot-worker', '.',
    dockerfile='stacks/flow.Dockerfile',
    target='flow-snapshot-worker',
    only=['flow/', 'stacks/flow.Dockerfile'],
)

docker_build('peerdb', '.',
    dockerfile='stacks/peerdb-server.Dockerfile',
    only=['nexus/', 'protos/', 'scripts/', 'stacks/peerdb-server.Dockerfile'],
    build_args={
        'BUILD_MODE': 'debug',
        'CARGO_FLAGS': '--no-default-features --features mysql',
    },
)

docker_build('peerdb-ui', '.',
    dockerfile='stacks/peerdb-ui.Dockerfile',
    target='dev',
    only=['ui/', 'stacks/peerdb-ui.Dockerfile', 'stacks/ui/'],
)

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
    'provision-mysql-gtid',
    cmd='./local_provision_scripts/mysql.sh peerdb-mysql-gtid 3306',
    labels=['Ancillary', 'Provisioning'],
    resource_deps=['mysql-gtid']
)

local_resource(
    'provision-mysql-pos',
    cmd='./local_provision_scripts/mysql.sh peerdb-mysql-pos 3307',
    labels=['Ancillary', 'Provisioning'],
    resource_deps=['mysql-pos']
)

local_resource(
    'provision-mariadb',
    cmd='./local_provision_scripts/mysql.sh peerdb-mariadb 3308',
    labels=['Ancillary', 'Provisioning'],
    resource_deps=['mariadb']
)

local_resource(
    'provision-postgres',
    cmd='./local_provision_scripts/postgres.sh',
    labels=['Ancillary', 'Provisioning'],
    resource_deps=['postgres']
)

# This is not defined as a resource as we need the file to be present
# when `docker_compose` loads the configuration (next line).
local('./generate-test-environment.sh')

docker_compose('./ancillary-docker-compose.yml', env_file='ancillary.env')

dc_resource('mongodb', labels=['Ancillary', 'DataStore'], links=[
    link('http://localhost:27017', 'MongoDB'),
])

dc_resource('clickhouse', labels=['Ancillary', 'DataStore'], links=[
    link('http://localhost:8123', 'ClickHouse HTTP'),
    link('http://localhost:9000', 'ClickHouse TCP'),
])

dc_resource('mysql-gtid', labels=['Ancillary', 'DataStore'], links=[
    link('http://localhost:3306', 'MySQL GTID'),
])

dc_resource('mysql-pos', labels=['Ancillary', 'DataStore'], links=[
    link('http://localhost:3307', 'MySQL File-Pos'),
])

dc_resource('mariadb', labels=['Ancillary', 'DataStore'], links=[
    link('http://localhost:3308', 'MariaDB'),
])

dc_resource('postgres', labels=['Ancillary', 'DataStore'], links=[
    link('http://localhost:5432', 'PostgreSQL'),
])