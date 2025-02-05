'use client';
import { MirrorType } from '@/app/dto/MirrorsDTO';
import { fetcher } from '@/app/utils/swr';
import { Label } from '@/lib/Label';
import { RowWithRadiobutton } from '@/lib/Layout';
import { ProgressCircle } from '@/lib/ProgressCircle';
import { RadioButton, RadioButtonGroup } from '@/lib/RadioButtonGroup';
import Link from 'next/link';
import { SetStateAction } from 'react';
import useSWR from 'swr';
import { MirrorCardStyle } from './styles';

export default function MirrorCards({
  mirrorType,
  setMirrorType,
}: {
  mirrorType: MirrorType;
  setMirrorType: (value: SetStateAction<MirrorType>) => void;
}) {
  const { data: cards, isLoading } = useSWR<
    {
      title: MirrorType;
      description: string;
      link: string;
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
          display: 'flex',
          alignItems: 'start',
          justifyContent: 'space-between',
          marginBottom: '1rem',
          columnGap: '1rem',
        }}
      >
        {cards.map((card, index) => {
          return (
            <label key={index} style={MirrorCardStyle}>
              <div>
                <RowWithRadiobutton
                  label={
                    <Label>
                      <div style={{ fontWeight: 'bold' }}>{card.title}</div>
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
                  color: 'teal',
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
