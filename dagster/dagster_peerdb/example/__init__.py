from dagster import Definitions, load_assets_from_modules, job

from . import assets
from dagster_peerdb import PeerDBResource, peerdb_execute_mirror

all_assets = load_assets_from_modules([assets])

peerdb_resource = PeerDBResource.configure_at_launch()


simple_mirror_op = peerdb_execute_mirror.configured(
    {
        "mirror_name": "simple_mirror_2",
    },
    name="simple_mirror",
)


@job(
    resource_defs={"peerdb": peerdb_resource},
)
def simple_mirror_job():
    simple_mirror_op()


defs = Definitions(
    assets=all_assets,
    jobs=[simple_mirror_job],
    resources={"peerdb": peerdb_resource},
)
