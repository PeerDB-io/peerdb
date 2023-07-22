import asyncio
import psycopg
from dagster import ConfigurableResource, get_dagster_logger
from temporalio.client import Client


class PeerDBResource(ConfigurableResource):
    peerdb_server_jdbc_url: str
    temporal_host_port: str

    def execute_mirror(
        self,
        mirror_name: str,
    ) -> str:
        log = get_dagster_logger()
        workflow_id = ""
        with psycopg.connect(self.peerdb_server_jdbc_url) as conn:
            with conn.cursor() as cur:
                cur.execute(f"EXECUTE MIRROR {mirror_name}")
                cur.execute(
                    f"SELECT workflow_id FROM flows WHERE name = '{mirror_name}'"
                )
                workflow_id = cur.fetchone()[0]
            log.info(f"started PeerDB workflow: {workflow_id}")
        asyncio.run(self.wait_for_workflow_completion(workflow_id))
        return workflow_id

    async def wait_for_workflow_completion(self, workflow_id: str) -> None:
        # sleep for 2 seconds to give the workflow time to start
        await asyncio.sleep(2)
        log = get_dagster_logger()
        client = await Client.connect(self.temporal_host_port, namespace="default")
        log.info(f"waiting for PeerDB workflow: {workflow_id}")
        handle = client.get_workflow_handle(workflow_id)
        await handle.result()
