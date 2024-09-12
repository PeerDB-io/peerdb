'use client';
import { useEffect, useState } from 'react';

export default function Supabase() {
  const urlParams = new URLSearchParams(window.location.search);
  const [databases, setDatabases] = useState<any[]>(null);

  useEffect(() => {
    fetch('/api/supabase', {
      method: 'POST',
      body: JSON.stringify({ code: urlParams.get('code') }),
      cache: 'no-store',
    })
      .then((res) => res.json())
      .then((dbs) => setDatabases(dbs));
  });
  return (
    <>
      {databases === null && 'Loading..'}
      {databases !== null && databases.map((db) => <a href='TODO'>{db}</a>)}
    </>
  );
}
