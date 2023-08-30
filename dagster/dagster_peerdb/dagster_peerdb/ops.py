from dagster import Config, In, Nothing, Out, Output, op, get_dagster_logger
from pydantic import Field

from .resources import PeerDBResource
from .types import PeerDBMirrorOutput


class PeerDBMirrorConfig(Config):
    mirror_name: str = Field(
        ...,
        description="The name of the mirror job to execute."
        "This job must already have been created by using the "
        "`CREATE MIRROR` commad with `disabled = true`.",
    )


@op(
    ins={"start_after": In(Nothing)},
    out=Out(
        PeerDBMirrorOutput,
        description=("The output of the peerdb mirror operation."),
    ),
)
def peerdb_execute_mirror(context, config: PeerDBMirrorConfig, peerdb: PeerDBResource):
    log = get_dagster_logger()
    workflow_id = peerdb.execute_mirror(config.mirror_name)
    log.info(f"Executed PeerDB workflow: {workflow_id}")
    output = PeerDBMirrorOutput(
        workflow_id=workflow_id,
    )
    return Output(output)
