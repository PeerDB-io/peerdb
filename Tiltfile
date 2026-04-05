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
    labels=['Provisioning'],
    resource_deps=['mongodb']
)

local_resource(
    'provision-clickhouse',
    cmd='./local_provision_scripts/clickhouse.sh',
    labels=['Provisioning'],
    resource_deps=['clickhouse']
)

local_resource(
    'provision-mysql-gtid',
    cmd='./local_provision_scripts/mysql.sh peerdb-mysql-gtid 3306',
    labels=['Provisioning'],
    resource_deps=['mysql-gtid']
)

local_resource(
    'provision-mysql-pos',
    cmd='./local_provision_scripts/mysql.sh peerdb-mysql-pos 3307',
    labels=['Provisioning'],
    resource_deps=['mysql-pos']
)

local_resource(
    'provision-mariadb',
    cmd='./local_provision_scripts/mysql.sh peerdb-mariadb 3308',
    labels=['Provisioning'],
    resource_deps=['mariadb']
)

local_resource(
    'provision-postgres',
    cmd='./local_provision_scripts/postgres.sh',
    labels=['Provisioning'],
    resource_deps=['postgres']
)

# This is not defined as a resource as we need the file to be present
# when `docker_compose` loads the configuration (next line).
local('./generate-test-environment.sh')

tiltfile_dir = config.main_path.replace('/Tiltfile', '')
docker_compose('./ancillary-docker-compose.yml', env_file=tiltfile_dir + '/ancillary.env')

# Data storages for tests, they are not automatically started to save resources.
# Their provisioning step resources are autostarted but blocked on the manual activation
# of their corresponding datastore.
# This way, users can choose which ones to start and when, depending on the tests they want to run.

dc_resource('clickhouse', labels=['DataStore'], links=[
    link('http://localhost:11123', 'ClickHouse HTTP'),
    link('http://localhost:11000', 'ClickHouse TCP'),
], auto_init=False)

dc_resource('mongodb', labels=['DataStore'], links=[
    link('http://localhost:11017', 'MongoDB'),
], auto_init=False)

dc_resource('mysql-gtid', labels=['DataStore'], links=[
    link('http://localhost:3306', 'MySQL GTID'),
], auto_init=False)

dc_resource('mysql-pos', labels=['DataStore'], links=[
    link('http://localhost:3307', 'MySQL File-Pos'),
], auto_init=False)

dc_resource('mariadb', labels=['DataStore'], links=[
    link('http://localhost:3308', 'MariaDB'),
], auto_init=False)

dc_resource('postgres', labels=['DataStore'], links=[
    link('http://localhost:5432', 'PostgreSQL'),
], auto_init=False)

# Monitoring and utility tools

dc_resource('dozzle', labels=['Monitoring'], links=[
    link('http://localhost:8118', 'Dozzle Container Monitor'),
])

# Tests launchers

def e2e_test(name, test_run, extra_deps=[]):
    local_resource(
        'e2e_' + name,
        cmd='cd flow && go clean -cache && env -f ../.env go test -v -run %s ./e2e/' % test_run,
        labels=['Test'],
        auto_init=False,
        resource_deps=['flow-api', 'flow-worker', 'catalog'] + extra_deps,
    )

# Postgres to ClickHouse generic tests
e2e_test('postgres', 'TestGenericCH_PG', ['provision-postgres'])

# MySQL to ClickHouse generic tests
e2e_test('mysql', 'TestGenericCH_MySQL', ['provision-mysql-gtid', 'provision-mysql-pos', 'provision-mariadb'])

# MongoDB to ClickHouse test suite
e2e_test('mongodb', 'TestMongoClickhouseSuite', ['provision-mongodb'])