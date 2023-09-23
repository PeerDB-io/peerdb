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


sync_to_mdv_op = peerdb_execute_mirror.configured(
    {
        "mirror_name": "simple_mirror_from_src_to_dst_raw_final_exec",
    },
    name="sync_to_mdv",
)

client_code_mapping_op = peerdb_execute_mirror.configured(
    {
        "mirror_name": "mirror_client_code_mapping",
    },
    name="mirror_client_code_mapping",
)

currency_mapping_op = peerdb_execute_mirror.configured(
    {
        "mirror_name": "mirror_currency_mapping",
    },
    name="mirror_currency_mapping",
)


direct_buys_months_op = dbt_run_op.alias(name="direct_buys_months")


@job
def mdv_normalize_load():
    dbt_output = direct_buys_months_op(start_after=[currency_mapping_op(start_after=[client_code_mapping_op(start_after=[sync_to_mdv_op()])])])
    # return dbt_output


defs = Definitions(
    assets=all_assets,
    jobs=[mdv_normalize_load],
    resources={
        "peerdb": peerdb_resource,
        "dbt": dbt_resource,
    },
)
