'use client';
import { Label } from '@/lib/Label';
import { SearchField } from '@/lib/SearchField';
import { useRouter, useSearchParams } from 'next/navigation';
import { Suspense, useEffect, useMemo, useState } from 'react';
import { SupabaseListProjectsResponse } from '../dto/PeersDTO';
import ProjectCard from './projectCard';
import { ProjectListStyle, ProjectsContainerStyle } from './styles';

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
  const router = useRouter();
  const [projects, setProjects] = useState<SupabaseListProjectsResponse[]>([]);
  const [searchQuery, setSearchQuery] = useState<string>('');

  const searchedProjects = useMemo(() => {
    return projects.filter((project) =>
      project.name.toLowerCase().includes(searchQuery)
    );
  }, [projects, searchQuery]);

  useEffect(() => {
    try {
      fetch('/api/supabase', {
        method: 'POST',
        body: JSON.stringify({ code: searchParams.get('code') }),
        cache: 'no-store',
      })
        .then((res) => res.json())
        .then((dbs: SupabaseListProjectsResponse[]) => {
          setProjects(dbs);
          if (dbs.length === 1) {
            router.push(
              `/peers/create/SUPABASE?host=${encodeURIComponent(
                dbs[0].database.host
              )}&name=${encodeURIComponent(dbs[0].name.toLowerCase().replaceAll('-', '_'))}&db=postgres`
            );
          }
        });
    } catch (e) {
      console.error(e);
    }
  }, [router, searchParams]);

  if (projects === null) return 'Loading..';
  return (
    <div style={ProjectsContainerStyle}>
      <Label as='label' variant='title2'>
        Select a Supabase project
      </Label>
      <Label colorName='lowContrast'>
        PeerDB will connect to the Supabase database for moving data
      </Label>
      <SearchField
        value={searchQuery}
        onChange={(e) => setSearchQuery(e.target.value)}
        placeholder='Search projects...'
      />
      <div style={ProjectListStyle}>
        {searchedProjects.map((project, i) => (
          <ProjectCard key={i} project={project} />
        ))}
      </div>
    </div>
  );
}
