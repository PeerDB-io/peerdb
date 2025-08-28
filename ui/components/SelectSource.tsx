'use client';
import { fetcher } from '@/app/utils/swr';
import TitleCase from '@/app/utils/titlecase';
import { Button } from '@/lib/Button/Button';
import { ProgressCircle } from '@/lib/ProgressCircle';
import Image from 'next/image';
import Link from 'next/link';
import useSWR from 'swr';
import { useTheme as useStyledTheme } from 'styled-components';
import { DBTypeToImageMapping } from './PeerComponent';

// label corresponds to PeerType
function SourceLabel({ label, url }: { label: string; url?: string }) {
  const theme = useStyledTheme();
  const peerLogo = DBTypeToImageMapping(label);
  return (
    <Button
      as={Link}
      href={url ?? `/peers/create/${label}`}
      style={{
        justifyContent: 'space-between',
        padding: '0.5rem',
        backgroundColor: theme.colors.base.surface.normal,
        borderRadius: '1rem',
        border: `1px solid ${theme.colors.base.border.subtle}`,
      }}
    >
      <Image
        src={peerLogo}
        alt='peer'
        width={20}
        height={20}
        objectFit='cover'
      />
      <div>{TitleCase(label)}</div>
    </Button>
  );
}

export default function SelectSource() {
  const theme = useStyledTheme();
  
  const gridContainerStyle = {
    display: 'flex',
    gap: '20px',
    flexWrap: 'wrap',
    border: `1px solid ${theme.colors.base.border.subtle}`,
    borderRadius: '20px',
    position: 'relative',
    padding: '20px',
    marginTop: '20px',
  } as const;
  
  const gridHeaderStyle = {
    position: 'absolute',
    top: '-15px',
    height: '30px',
    display: 'flex',
    alignItems: 'center',
    color: theme.colors.base.text.highContrast,
    backgroundColor: theme.colors.base.background.normal,
    border: `1px solid ${theme.colors.base.border.subtle}`,
    borderRadius: '15px',
    marginLeft: '10px',
    paddingLeft: '10px',
    paddingRight: '10px',
  } as const;
  const { data: dbTypes, isLoading } = useSWR<
    [string, ...Array<string | { label: string; url: string }>][]
  >('/api/peer-types', fetcher);
  if (!dbTypes || isLoading) {
    return <ProgressCircle variant={'determinate_progress_circle'} />;
  }

  return dbTypes.map(([category, ...items]) => (
    <div key={category} style={gridContainerStyle}>
      <div style={gridHeaderStyle}>{category}</div>
      {items.map((item, i) =>
        typeof item === 'string' ? (
          <SourceLabel key={i} label={item} />
        ) : (
          <SourceLabel key={i} label={item.label} url={item.url} />
        )
      )}
    </div>
  ));
}
