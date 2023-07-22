import asyncio
import psycopg
from dagster import ConfigurableResource
from temporalio.client import Client


class PeerDBResource(ConfigurableResource):
    peerdb_server_jdbc_url: str

    def execute_mirror(
        self,
        mirror_name: str,
    ) -> str:
        workflow_id = ""
        with psycopg.connect(self.peerdb_server_jdbc_url) as conn:
            with conn.cursor() as cur:
                cur.execute(f"EXECUTE MIRROR {mirror_name}")
                statusmessage = cur.statusmessage
                workflow_id = statusmessage.split(" ")[-1]
        asyncio.run(self.wait_for_workflow_completion(workflow_id))

    async def wait_for_workflow_completion(self, workflow_id: str) -> None:
        client = await Client.connect(self.temporal_host_port, namespace="default")
        handle = await client.get_workflow_handle(workflow_id)
        await handle.result()
