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
