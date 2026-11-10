'use client';
import { MirrorType } from '@/app/dto/MirrorsDTO';
import { fetcher } from '@/app/utils/swr';
import { Badge } from '@/lib/Badge';
import { Label } from '@/lib/Label';
import { RowWithRadiobutton } from '@/lib/Layout';
import { ProgressCircle } from '@/lib/ProgressCircle';
import { RadioButton, RadioButtonGroup } from '@/lib/RadioButtonGroup';
import Link from 'next/link';
import { SetStateAction } from 'react';
import { useTheme } from 'styled-components';
import useSWR from 'swr';

export default function MirrorCards({
  mirrorType,
  setMirrorType,
}: {
  mirrorType: MirrorType;
  setMirrorType: (value: SetStateAction<MirrorType>) => void;
}) {
  const theme = useTheme();
  const { data: cards, isLoading } = useSWR<
    {
      title: MirrorType;
      description: string;
      link: string;
      deprecated?: boolean;
    }[]
  >('/api/mirror-types', fetcher);

  if (!cards || isLoading) {
    return <ProgressCircle variant='determinate_progress_circle' />;
  }

  return (
    <RadioButtonGroup
      value={mirrorType}
      onValueChange={(value: MirrorType) => setMirrorType(value)}
    >
      <div
        style={{
          display: 'grid',
          gridTemplateColumns: 'repeat(3, minmax(0, 1fr))',
          alignItems: 'stretch',
          columnGap: '0.75rem',
        }}
      >
        {cards.map((card, index) => {
          const selected = card.title === mirrorType;
          return (
            <label
              key={index}
              style={{
                cursor: 'pointer',
                padding: '0.75rem 1rem',
                display: 'flex',
                flexDirection: 'column',
                justifyContent: 'space-between',
                rowGap: '0.5rem',
                borderRadius: '0.7rem',
                background: selected
                  ? theme.colors.accent.background.subtle
                  : theme.colors.base.background.normal,
                border: `1px solid ${
                  selected
                    ? theme.colors.accent.fill.normal
                    : theme.colors.base.border.subtle
                }`,
                boxShadow: selected
                  ? `inset 0 0 0 1px ${theme.colors.accent.fill.normal}`
                  : 'none',
                opacity: card.deprecated && !selected ? 0.8 : 1,
                transition: 'transform 0.18s, border-color 0.18s',
              }}
              onMouseEnter={(e) =>
                (e.currentTarget.style.transform = 'translateY(-2px)')
              }
              onMouseLeave={(e) => (e.currentTarget.style.transform = 'none')}
            >
              <div>
                <RowWithRadiobutton
                  label={
                    <Label>
                      <div
                        style={{
                          fontWeight: 'bold',
                          display: 'flex',
                          alignItems: 'center',
                          gap: '0.5rem',
                        }}
                      >
                        {card.title}
                        {card.deprecated && (
                          <Badge variant='destructive'>
                            <Label
                              as='label'
                              style={{ fontSize: 13, padding: 0 }}
                            >
                              Deprecated
                            </Label>
                          </Badge>
                        )}
                      </div>
                    </Label>
                  }
                  action={<RadioButton value={card.title} />}
                />
                <Label>
                  <div style={{ fontSize: 14 }}>{card.description}</div>
                </Label>
              </div>
              <Label
                as={Link}
                target='_blank'
                style={{
                  color: theme.colors.accent.text.lowContrast,
                  cursor: 'pointer',
                  width: 'fit-content',
                }}
                href={card.link}
              >
                Learn more
              </Label>
            </label>
          );
        })}
      </div>
    </RadioButtonGroup>
  );
}
