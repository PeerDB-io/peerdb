from dagster import Definitions, load_assets_from_modules, job

from dagster_peerdb import PeerDBResource, peerdb_execute_mirror
from dagster_dbt import dbt_cli_resource, dbt_run_op

from . import assets
from .assets import DBT_PROJECT_DIR

all_assets = load_assets_from_modules([assets])

peerdb_resource = PeerDBResource.configure_at_launch()

dbt_resource = dbt_cli_resource.configured(
    {
        "project_dir": DBT_PROJECT_DIR,
        "profiles_dir": DBT_PROJECT_DIR,
    },
)


simple_mirror_op = peerdb_execute_mirror.configured(
    {
        "mirror_name": "simple_mirror_from_src_to_dst",
    },
    name="simple_mirror",
)


events_in_usa_op = dbt_run_op.alias(name="events_in_usa_op")


@job(
    resource_defs={
        "peerdb": peerdb_resource,
        "dbt": dbt_resource,
    },
)
def simple_mirror_transform_job():
    dbt_output = events_in_usa_op(start_after=[simple_mirror_op()])
    # return dbt_output


defs = Definitions(
    assets=all_assets,
    jobs=[simple_mirror_transform_job],
    resources={
        "peerdb": peerdb_resource,
        "dbt": dbt_resource,
    },
)
