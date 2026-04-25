update_settings(max_parallel_updates=10)

allow_k8s_contexts(k8s_context()) # to unblock local() in local set-ups with a Kubernetes context configured, like Docker Desktop

docker_compose('./docker-compose-dev.yml')

local_resource(
    'preflight',
    cmd='./scripts/preflight-local-dev.sh',
    labels=['Setup'],
)

local_resource(
    'proto-gen',
    cmd='./generate-protos.sh',
    deps=['./protos'],
    labels=['PeerDB'],
)

dc_resource('temporal-ui', labels=['PeerDB'], links=[
    link('http://localhost:8085', 'Temporal UI'),
])
dc_resource('catalog', labels=['PeerDB'])
dc_resource('temporal', labels=['PeerDB'])
dc_resource('temporal-admin-tools', labels=['PeerDB'])
dc_resource('minio', labels=['PeerDB'])

flow_env = ' '.join([
    'PEERDB_CATALOG_HOST=localhost',
    'PEERDB_CATALOG_PORT=9901',
    'PEERDB_CATALOG_USER=postgres',
    'PEERDB_CATALOG_PASSWORD=postgres',
    'PEERDB_CATALOG_DATABASE=postgres',
    # For Temporal Cloud, this will look like:
    # <yournamespace>.<id>.tmprl.cloud:7233
    'TEMPORAL_HOST_PORT=localhost:7233',
    'PEERDB_TEMPORAL_NAMESPACE=default',
    # For the below 2 cert and key variables,
    # paste as base64 encoded strings.
    'TEMPORAL_CLIENT_CERT=${TEMPORAL_CLIENT_CERT:-}',
    'TEMPORAL_CLIENT_KEY=${TEMPORAL_CLIENT_KEY:-}',
    # For GCS, these will be your HMAC keys instead
    # For more information:
    # https://cloud.google.com/storage/docs/authentication/managing-hmackeys
    'AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID:-}',
    'AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY:-}',
    # For GCS, set this to "auto" without the quotes
    'AWS_REGION=${AWS_REGION:-}',
    # For GCS, set this as: https://storage.googleapis.com
    'AWS_ENDPOINT=${AWS_ENDPOINT:-}',
    'PEERDB_CLICKHOUSE_AWS_CREDENTIALS_AWS_ACCESS_KEY_ID=_peerdb_minioadmin',
    'PEERDB_CLICKHOUSE_AWS_CREDENTIALS_AWS_SECRET_ACCESS_KEY=_peerdb_minioadmin',
    'PEERDB_CLICKHOUSE_AWS_CREDENTIALS_AWS_REGION=us-east-1',
    # ClickHouse runs in docker; it must reach minio via host-gateway alias.
    'PEERDB_CLICKHOUSE_AWS_CREDENTIALS_AWS_ENDPOINT_URL_S3=http://host.docker.internal:9001',
    'PEERDB_CLICKHOUSE_AWS_S3_BUCKET_NAME=peerdb',
    'ENABLE_PROFILING=true',
])

flow_deps = ['flow/']
flow_ignore = ['flow/e2e/', 'flow/**/*_test.go', 'flow/peer-flow']

local_resource(
    'flow-compile',
    cmd='cd flow && go build -o peer-flow .',
    deps=flow_deps,
    ignore=flow_ignore,
    resource_deps=['preflight', 'proto-gen'],
    labels=['PeerDB'],
    allow_parallel=True,
)

local_resource(
    'flow-api',
    serve_cmd='cd flow && %s PPROF_PORT=6061 ./peer-flow api --port 8112 --gateway-port 8113' % flow_env,
    deps=['flow/peer-flow'],
    resource_deps=['preflight', 'flow-compile', 'catalog', 'temporal-admin-tools'],
    labels=['PeerDB'],
    allow_parallel=True,
    links=[
        link('http://localhost:8112', 'Flow API gRPC'),
        link('http://localhost:8113', 'Flow API HTTP'),
    ],
)

local_resource(
    'flow-worker',
    serve_cmd='cd flow && %s PPROF_PORT=6062 ./peer-flow worker' % flow_env,
    deps=['flow/peer-flow'],
    resource_deps=['preflight', 'flow-compile', 'catalog', 'temporal-admin-tools'],
    labels=['PeerDB'],
    allow_parallel=True,
)

local_resource(
    'flow-snapshot-worker',
    serve_cmd='cd flow && %s PPROF_PORT=6063 ./peer-flow snapshot-worker' % flow_env,
    deps=['flow/peer-flow'],
    resource_deps=['preflight', 'flow-compile', 'catalog', 'temporal-admin-tools'],
    labels=['PeerDB'],
    allow_parallel=True,
)

peerdb_server_env = ' '.join([
    'PEERDB_CATALOG_HOST=localhost',
    'PEERDB_CATALOG_PORT=9901',
    'PEERDB_CATALOG_USER=postgres',
    'PEERDB_CATALOG_PASSWORD=postgres',
    'PEERDB_CATALOG_DATABASE=postgres',
    'PEERDB_PASSWORD=peerdb',
    'PEERDB_FLOW_SERVER_ADDRESS=grpc://localhost:8112',
    'RUST_LOG=info',
    'RUST_BACKTRACE=1',
])

local_resource(
    'peerdb',
    serve_cmd='cd nexus && %s cargo run --bin peerdb-server --no-default-features --features mysql' % peerdb_server_env,
    deps=['nexus/'],
    ignore=['nexus/target/'],
    resource_deps=['preflight', 'proto-gen', 'catalog', 'flow-api'],
    labels=['PeerDB'],
    allow_parallel=True,
    links=[link('postgres://localhost:9900', 'PeerDB SQL')],
)

local_resource(
    'peerdb-ui-deps',
    cmd='cd ui && npm ci',
    deps=['ui/package.json', 'ui/package-lock.json'],
    labels=['PeerDB'],
    resource_deps=['preflight'],
    allow_parallel=True,
)

peerdb_ui_env = ' '.join([
    'PEERDB_CATALOG_HOST=localhost',
    'PEERDB_CATALOG_PORT=9901',
    'PEERDB_CATALOG_USER=postgres',
    'PEERDB_CATALOG_PASSWORD=postgres',
    'PEERDB_CATALOG_DATABASE=postgres',
    'PEERDB_FLOW_SERVER_HTTP=http://localhost:8113',
    'PEERDB_PASSWORD=peerdb',
    'NEXTAUTH_SECRET=__changeme__',
    'NEXTAUTH_URL=http://localhost:3030',
    'PEERDB_EXPERIMENTAL_ENABLE_SCRIPTING=true',
    'PORT=3030',
])

local_resource(
    'peerdb-ui',
    serve_cmd='cd ui && %s npx next dev --webpack' % peerdb_ui_env,
    resource_deps=['preflight', 'proto-gen', 'peerdb-ui-deps', 'flow-api'],
    labels=['PeerDB'],
    allow_parallel=True,
    links=[link('http://localhost:3030', 'PeerDB UI')],
)


# Ancillary services

local_resource(
    'provision-mongodb',
    cmd='./local_provision_scripts/mongodb.sh',
    labels=['Ancillary-DB-Provisioning'],
    resource_deps=['mongodb']
)

local_resource(
    'provision-clickhouse',
    cmd='./local_provision_scripts/clickhouse.sh',
    labels=['Ancillary-DB-Provisioning'],
    resource_deps=['clickhouse']
)

local_resource(
    'provision-mysql-gtid',
    cmd='./local_provision_scripts/mysql.sh peerdb-mysql-gtid',
    labels=['Ancillary-DB-Provisioning'],
    resource_deps=['mysql-gtid']
)

local_resource(
    'provision-mysql-pos',
    cmd='./local_provision_scripts/mysql.sh peerdb-mysql-pos',
    labels=['Ancillary-DB-Provisioning'],
    resource_deps=['mysql-pos']
)

local_resource(
    'provision-mariadb',
    cmd='./local_provision_scripts/mysql.sh peerdb-mariadb',
    labels=['Ancillary-DB-Provisioning'],
    resource_deps=['mariadb']
)

local_resource(
    'provision-postgres',
    cmd='./local_provision_scripts/postgres.sh',
    labels=['Ancillary-DB-Provisioning'],
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

dc_resource('clickhouse', labels=['Ancillary-DB'], links=[
    link('http://localhost:11123', 'ClickHouse HTTP'),
    link('http://localhost:11000', 'ClickHouse TCP'),
], auto_init=False)

dc_resource('mongodb', labels=['Ancillary-DB'], links=[
    link('http://localhost:11017', 'MongoDB'),
], auto_init=False)

dc_resource('mysql-gtid', labels=['Ancillary-DB'], links=[
    link('http://localhost:3306', 'MySQL GTID'),
], auto_init=False)

dc_resource('mysql-pos', labels=['Ancillary-DB'], links=[
    link('http://localhost:3307', 'MySQL File-Pos'),
], auto_init=False)

dc_resource('mariadb', labels=['Ancillary-DB'], links=[
    link('http://localhost:3308', 'MariaDB'),
], auto_init=False)

dc_resource('postgres', labels=['Ancillary-DB'], links=[
    link('http://localhost:5432', 'PostgreSQL'),
], auto_init=False)

local_resource(
    'all-test-resources',
    cmd=' '.join([
        "resources=$(tilt --port 10352 get uiresource -o json |",
        "jq -r '.items[] | select(.metadata.labels[\"Ancillary-DB\"] or .metadata.labels[\"Ancillary-TestInfra\"]) | .metadata.name');",
        "tilt --port 10352 enable $resources &&",
        "for r in $resources; do tilt --port 10352 trigger $r; done",
    ]),
    labels=['Ancillary-Utilities'],
    auto_init=False,
)

# Monitoring and utility tools

dc_resource('dozzle', labels=['Monitoring'], links=[
    link('http://localhost:8118', 'Dozzle Container Monitor'),
])

# Test services: Services supporting test execution that are not data stores, like proxies, mock servers, etc.

dc_resource('toxiproxy', labels=['Ancillary-TestInfra'], auto_init=False)
dc_resource('openssh', labels=['Ancillary-TestInfra'], auto_init=False)

# Cleanup

load('ext://uibutton', 'cmd_button', 'location')
cmd_button(
    'cleanup-ancillary-volumes',
    argv=['sh', '-c', ' '.join([
        "for v in $(yq '.volumes | keys[]' ancillary-docker-compose.yml);",
        'do if docker volume rm peerdb_$v;',
        'then echo "Removed $v";',
        'else echo "Failed to remove $v (may be in use or not exist)";',
        'fi; done',
    ])],
    text='Wipe ancillary volumes',
    icon_name='delete',
    location=location.NAV
)

cmd_button(
    'Clean-up test code caches',
    argv=['sh', '-c', 'cd flow && go clean -cache'],
    text='Clean-up test code caches',
    icon_name='delete_sweep',
    resource='Test',
    location=location.NAV
)

# Tests launchers

def e2e_test(name, test_run, extra_deps=[], vars_overrides={}):
    overrides_str = ' '.join(['%s=%s' % (var, value) for var, value in vars_overrides.items()])
    local_resource(
        'e2e_' + name,
        cmd='cd flow && %s go test -count=1 -v -run %s ./e2e/' % (overrides_str, test_run),
        labels=['Test'],
        auto_init=False,
        resource_deps=['flow-api', 'flow-worker', 'catalog', 'provision-clickhouse'] + extra_deps,
        allow_parallel=True,
    )

def connector_test(connector, extra_deps=[], vars_overrides={}):
    overrides_str = ' '.join(['%s=%s' % (var, value) for var, value in vars_overrides.items()])
    local_resource(
        'connector_' + connector,
        cmd='cd flow && %s go test -count=1 -v ./connectors/%s/...' % (overrides_str, connector),
        labels=['Test'],
        auto_init=False,
        resource_deps=['catalog'] + extra_deps,
        allow_parallel=True,
    )

# These are overrides to provide different MySQL flavors with the same test definitions.

def resolve_env(var_name):
    for line in str(read_file('.env')).splitlines():
        if line.startswith(var_name + '='):
            return line.strip().split('=', 1)[1]
    return None

mysql_gtid_vars = {
    'CI_MYSQL_PORT': resolve_env('CI_MYSQL_GTID_PORT'),
    'CI_MYSQL_VERSION': resolve_env('CI_MYSQL_GTID_VERSION'),
}
mysql_pos_vars = {
    'CI_MYSQL_PORT': resolve_env('CI_MYSQL_POS_PORT'),
    'CI_MYSQL_VERSION': resolve_env('CI_MYSQL_POS_VERSION'),
}
mariadb_vars = {
    'CI_MYSQL_PORT': resolve_env('CI_MARIADB_PORT'),
    'CI_MYSQL_VERSION': resolve_env('CI_MARIADB_VERSION'),
}

# Generic e2e tests

# Postgres to ClickHouse generic tests
e2e_test('postgres', 'TestGenericCH_PG', ['provision-postgres'])

# MySQL GTID to ClickHouse generic tests
e2e_test('mysql-gtid', 'TestGenericCH_MySQL', ['provision-mysql-gtid'], vars_overrides=mysql_gtid_vars)

# MySQL Pos to ClickHouse generic tests
e2e_test('mysql-pos', 'TestGenericCH_MySQL', ['provision-mysql-pos'], vars_overrides=mysql_pos_vars)

# MariaDB to ClickHouse generic tests
e2e_test('mariadb', 'TestGenericCH_MySQL', ['provision-mariadb'], vars_overrides=mariadb_vars)

# MongoDB to ClickHouse test suite
e2e_test('mongodb', 'TestMongoClickhouseSuite', ['provision-mongodb'])

# Switchboard tests

e2e_test('switchboard-postgres', 'TestSwitchboardPostgres', ['provision-postgres'])

e2e_test('switchboard-mysql-gtid', 'TestSwitchboardMySQL', ['provision-mysql-gtid'], vars_overrides=mysql_gtid_vars)
e2e_test('switchboard-mysql-pos', 'TestSwitchboardMySQL', ['provision-mysql-pos'], vars_overrides=mysql_pos_vars)
e2e_test('switchboard-mariadb', 'TestSwitchboardMySQL', ['provision-mariadb'], vars_overrides=mariadb_vars)

e2e_test('switchboard-mongodb', 'TestSwitchboardMongo', ['provision-mongodb'])

# Peer flow E2E

e2e_test('peer-flow-postgres', '^TestPeerFlowE2ETestSuitePG_CH$', ['provision-postgres'])

e2e_test('peer-flow-mysql-gtid', '^TestPeerFlowE2ETestSuiteMySQL_CH$', ['provision-mysql-gtid'], vars_overrides=mysql_gtid_vars)
e2e_test('peer-flow-mysql-pos', '^TestPeerFlowE2ETestSuiteMySQL_CH$', ['provision-mysql-pos'], vars_overrides=mysql_pos_vars)
e2e_test('peer-flow-mariadb', '^TestPeerFlowE2ETestSuiteMySQL_CH$', ['provision-mariadb'], vars_overrides=mariadb_vars)

# API e2e tests

e2e_test('api-postgres', 'TestApiPg', ['provision-postgres'])

e2e_test('api-mysql-gtid', 'TestApiMy', ['provision-mysql-gtid', 'provision-postgres'], vars_overrides=mysql_gtid_vars)
e2e_test('api-mysql-pos', 'TestApiMy', ['provision-mysql-pos', 'provision-postgres'], vars_overrides=mysql_pos_vars)
e2e_test('api-mariadb', 'TestApiMy', ['provision-mariadb', 'provision-postgres'], vars_overrides=mariadb_vars)

e2e_test('api-mongodb', 'TestApiMongo', ['provision-mongodb'])

# Connectors tests

connector_test('postgres', ['provision-postgres'])
connector_test('mongo', ['provision-mongodb'])
connector_test('mysql', ['provision-mysql-gtid'])
connector_test('clickhouse', ['provision-clickhouse'])
