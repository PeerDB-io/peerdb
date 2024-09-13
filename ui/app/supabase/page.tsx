'use client';
import { useSearchParams } from 'next/navigation';
import { Suspense, useEffect, useState } from 'react';

// https://nextjs.org/docs/messages/missing-suspense-with-csr-bailout
export default function Supabase() {
  return (
    <Suspense>
      <SupabaseCore />
    </Suspense>
  );
}

function SupabaseCore() {
  const searchParams = useSearchParams();
  const [databases, setDatabases] = useState<any[] | null>(null);

  useEffect(() => {
    fetch('/api/supabase', {
      method: 'POST',
      body: JSON.stringify({ code: searchParams.get('code') }),
      cache: 'no-store',
    })
      .then((res) => res.json())
      .then((dbs) => setDatabases(dbs));
  });

  if (databases === null) return 'Loading..';
  return databases.map((db, i) => (
    <a
      key={i}
      style={{ display: 'block' }}
      href={`/peers/create/SUPABASE?host=${encodeURIComponent(db.host)}&name=${encodeURIComponent(db.name)}&db=postgres`}
    >
      {db.name}
    </a>
  ));
}
