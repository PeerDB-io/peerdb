// Calls for choosing the ClickHouse database a mirror writes to.
// The database belongs to the peer, so choosing another one yields the virtual
// destination `<peer>__<database>`: no peer is created, the backend resolves it
// from the peer's own connection.

export async function fetchClickHouseDatabases(
  peerName: string
): Promise<{ databases: string[]; currentDatabase: string }> {
  const res = await fetch(
    `/api/v1/peers/clickhouse/databases?peerName=${encodeURIComponent(peerName)}`,
    { cache: 'no-store' }
  );
  if (!res.ok) {
    throw new Error(await errorMessage(res));
  }
  const body = await res.json();
  return {
    databases: body.databases ?? [],
    currentDatabase: body.currentDatabase ?? '',
  };
}

export async function useClickHouseDatabase(
  peerName: string,
  database: string,
  createDatabase: boolean
): Promise<string> {
  const res = await fetch('/api/v1/peers/clickhouse/use_database', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ peerName, database, createDatabase }),
  });
  if (!res.ok) {
    throw new Error(await errorMessage(res));
  }
  return (await res.json()).peerName;
}

async function errorMessage(res: Response): Promise<string> {
  try {
    const body = await res.json();
    return body.message ?? `Request failed (${res.status})`;
  } catch {
    return `Request failed (${res.status})`;
  }
}
