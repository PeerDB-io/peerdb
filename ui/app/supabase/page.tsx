'use client';
import { useEffect, useState } from 'react';

export default function Supabase() {
  const urlParams = new URLSearchParams(window.location.search);
  const [databases, setDatabases] = useState<any[] | null>(null);

  useEffect(() => {
    fetch('/api/supabase', {
      method: 'POST',
      body: JSON.stringify({ code: urlParams.get('code') }),
      cache: 'no-store',
    })
      .then((res) => res.json())
      .then((dbs) => setDatabases(dbs));
  });

  if (databases === null) return 'Loading..';
  return databases.map((db) => (
    <a href='TODO'>
      {db.name} {db.host}
    </a>
  ));
}
