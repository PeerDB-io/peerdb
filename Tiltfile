update_settings(max_parallel_updates=10)

allow_k8s_contexts(k8s_context()) # to unblock local() in local set-ups with a Kubernetes context configured, like Docker Desktop

def resolve_env(var_name, default=None):
    for line in str(read_file('.env')).splitlines():
        if line.startswith(var_name + '='):
            return line.strip().split('=', 1)[1]
    return default

docker_compose('./docker-compose-dev.yml', project_name='peerdb-' + resolve_env('DEFAULT_TILT_PORT', '10350'), env_file='.env')

peerbd_ui_port = resolve_env('PEERBD_UI_PORT', '3030')
temporal_port = resolve_env('TEMPORAL_PORT', '7233')
temporal_ui_port = resolve_env('TEMPORAL_UI_PORT', '8085')
flow_api_grpc_port = resolve_env('FLOW_API_PORT', '8112')
flow_api_http_port = resolve_env('FLOW_API_HTTP_PORT', '8113')
docker_go_debug_port_flow_worker = resolve_env('DOCKER_GO_DEBUG_PORT_FLOW_WORKER', '4001')
docker_go_debug_port_flow_snapshot_worker = resolve_env('DOCKER_GO_DEBUG_PORT_FLOW_SNAPSHOT_WORKER', '4002')
docker_go_debug_port_flow_api = resolve_env('DOCKER_GO_DEBUG_PORT_FLOW_API', '4003')

flow_ignore = ['flow/e2e/', 'flow/**/*_test.go']

docker_build('flow-api', '.',
    dockerfile='stacks/flow.Dockerfile',
    target='flow-api-debug' if resolve_env('DOCKER_GO_DEBUG_FLOW_API') in ('1', 'true') else 'flow-api',
    only=['flow/', 'stacks/flow.Dockerfile'],
    ignore=flow_ignore,
    build_args={'DEBUG_BUILD': resolve_env('DOCKER_GO_DEBUG_FLOW_API',''),'PEERDB_VERSION_SHA_SHORT': os.getenv('PEERDB_VERSION_SHA_SHORT', 'unknown')},
)

docker_build('flow-worker', '.',
    dockerfile='stacks/flow.Dockerfile',
    target='flow-worker-debug' if resolve_env('DOCKER_GO_DEBUG_FLOW_WORKER') in ('1', 'true') else 'flow-worker',
    only=['flow/', 'stacks/flow.Dockerfile'],
    build_args={'DEBUG_BUILD': resolve_env('DOCKER_GO_DEBUG_FLOW_WORKER','')},
    ignore=flow_ignore,
)

docker_build('flow-snapshot-worker', '.',
    dockerfile='stacks/flow.Dockerfile',
    target='flow-snapshot-worker-debug' if resolve_env('DOCKER_GO_DEBUG_FLOW_SNAPSHOT_WORKER') in ('1', 'true') else 'flow-snapshot-worker',
    only=['flow/', 'stacks/flow.Dockerfile'],
    build_args={'DEBUG_BUILD': resolve_env('DOCKER_GO_DEBUG_FLOW_SNAPSHOT_WORKER','')},
    ignore=flow_ignore,
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
    link('http://localhost:' + str(peerbd_ui_port), 'PeerDB UI'),
])
dc_resource('flow-api', resource_deps=['proto-gen'], labels=['PeerDB'], links=[
    link('http://localhost:' + str(flow_api_grpc_port), 'Flow API gRPC'),
    link('http://localhost:' + str(flow_api_http_port), 'Flow API HTTP'),
])
dc_resource('temporal-ui', labels=['PeerDB'], links=[
    link('http://localhost:' + str(temporal_ui_port), 'Temporal UI'),
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

local_resource(
    'provision-postgres2',
    cmd='./local_provision_scripts/postgres.sh peerdb-postgres2',
    labels=['Ancillary-DB-Provisioning'],
    resource_deps=['postgres2']
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

dc_resource('postgres2', labels=['Ancillary-DB'], links=[
    link('http://localhost:5437', 'PostgreSQL (secondary)'),
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
