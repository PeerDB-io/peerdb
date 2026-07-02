'use client';
import { fetcher } from '@/app/utils/swr';
import TitleCase from '@/app/utils/titlecase';
import { Badge } from '@/lib/Badge';
import { Button } from '@/lib/Button/Button';
import { Label } from '@/lib/Label';
import { ProgressCircle } from '@/lib/ProgressCircle';
import Image from 'next/image';
import Link from 'next/link';
import { useTheme as useStyledTheme } from 'styled-components';
import useSWR from 'swr';
import { DBTypeToImageMapping } from './PeerComponent';

// label corresponds to PeerType
function SourceLabel({
  label,
  url,
  deprecated,
  deprecatedRole,
}: {
  label: string;
  url?: string;
  deprecated?: boolean;
  deprecatedRole?: 'destination';
}) {
  const theme = useStyledTheme();
  const peerLogo = DBTypeToImageMapping(label);
  // Uniform "Deprecated" badge; an asterisk flags connectors deprecated only
  // as a destination (explained by the footnote below the category).
  const deprecatedText =
    deprecatedRole === 'destination' ? 'Deprecated*' : 'Deprecated';
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
      <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
        <div>{TitleCase(label)}</div>
        {deprecated && (
          <Badge variant='destructive'>
            <Label as='label' style={{ fontSize: 13, padding: 0 }}>
              {deprecatedText}
            </Label>
          </Badge>
        )}
      </div>
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

  const footnoteStyle = {
    flexBasis: '100%',
    fontSize: '12px',
    color: theme.colors.base.text.lowContrast,
  } as const;
  const { data: dbTypes, isLoading } = useSWR<
    [
      string,
      ...Array<
        | string
        | {
            label: string;
            url?: string;
            deprecated?: boolean;
            deprecatedRole?: 'destination';
          }
      >,
    ][]
  >('/api/peer-types', fetcher);
  if (!dbTypes || isLoading) {
    return <ProgressCircle variant={'determinate_progress_circle'} />;
  }

  return dbTypes.map(([category, ...items]) => {
    const hasDestinationDeprecation = items.some(
      (item) =>
        typeof item !== 'string' && item.deprecatedRole === 'destination'
    );
    return (
      <div key={category} style={gridContainerStyle}>
        <div style={gridHeaderStyle}>{category}</div>
        {items.map((item, i) =>
          typeof item === 'string' ? (
            <SourceLabel key={i} label={item} />
          ) : (
            <SourceLabel
              key={i}
              label={item.label}
              url={item.url}
              deprecated={item.deprecated}
              deprecatedRole={item.deprecatedRole}
            />
          )
        )}
        {hasDestinationDeprecation && (
          <div style={footnoteStyle}>* Deprecated as a destination only.</div>
        )}
      </div>
    );
  });
}
