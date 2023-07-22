from dagster import Definitions, load_assets_from_modules, job

from . import assets
from dagster_peerdb import PeerDBResource, peerdb_execute_mirror

all_assets = load_assets_from_modules([assets])

peerdb_resource = PeerDBResource.configure_at_launch()


sql_server_to_postgres_mirror_op = peerdb_execute_mirror.configured(
    {
        "mirror_name": "simple_mirror_1",
    },
    name="simple_mirror_1",
)


@job(
    resource_defs={"peerdb": peerdb_resource},
)
def sql_server_to_postgres():
    sql_server_to_postgres_mirror_op()


defs = Definitions(
    assets=all_assets,
    jobs=[sql_server_to_postgres],
    resources={"peerdb": peerdb_resource},
)
