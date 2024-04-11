'use client';
import { MirrorType } from '@/app/dto/MirrorsDTO';
import { Label } from '@/lib/Label';
import { RowWithRadiobutton } from '@/lib/Layout';
import { RadioButton, RadioButtonGroup } from '@/lib/RadioButtonGroup';
import Link from 'next/link';
import { SetStateAction } from 'react';
import { MirrorCardStyle } from './styles';

const MirrorCards = ({
  mirrorType,
  setMirrorType,
}: {
  mirrorType: MirrorType;
  setMirrorType: (value: SetStateAction<MirrorType>) => void;
}) => {
  const cards = [
    {
      title: MirrorType.CDC,
      description:
        'Change-data Capture or CDC refers to replication of changes on the source table to the target table with initial load. This is recommended.',
      link: 'https://docs.peerdb.io/usecases/Real-time%20CDC/overview',
    },
    {
      title: MirrorType.QRep,
      description:
        'Query Replication allows you to specify a set of rows to be synced via a SELECT query.',
      link: 'https://docs.peerdb.io/usecases/Streaming%20Query%20Replication/overview',
    },
    {
      title: MirrorType.XMin,
      description:
        'XMIN mode uses the xmin system column of PostgreSQL as a watermark column for replication.',
      link: 'https://docs.peerdb.io/sql/commands/create-mirror#xmin-query-replication',
    },
  ];
  return (
    <RadioButtonGroup
      value={mirrorType}
      onValueChange={(value: MirrorType) => setMirrorType(value)}
    >
      <div
        style={{
          display: 'flex',
          alignItems: 'start',
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
};

export default MirrorCards;
