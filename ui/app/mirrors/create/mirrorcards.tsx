'use client';
import { Label } from '@/lib/Label';
import { RowWithRadiobutton } from '@/lib/Layout';
import { RadioButton, RadioButtonGroup } from '@/lib/RadioButtonGroup';
import Link from 'next/link';
import { SetStateAction } from 'react';

const MirrorCards = ({
  setMirrorType,
}: {
  setMirrorType: (value: SetStateAction<string>) => void;
}) => {
  const cards = [
    {
      title: 'CDC',
      description:
        'Change-data Capture or CDC refers to replication of changes on the source table to the target table with initial load. This is recommended.',
      link: 'https://docs.peerdb.io/usecases/Real-time%20CDC/overview',
    },
    {
      title: 'Query Replication',
      description:
        'Query Replication allows you to specify a set of rows to be synced via a SELECT query.',
      link: 'https://docs.peerdb.io/usecases/Streaming%20Query%20Replication/overview',
    },
    {
      title: 'XMIN',
      description:
        'XMIN mode uses the xmin system column of PostgreSQL as a watermark column for replication.',
      link: 'https://docs.peerdb.io/sql/commands/create-mirror#xmin-query-replication',
    },
  ];
  return (
    <RadioButtonGroup onValueChange={(value: string) => setMirrorType(value)}>
      <div
        style={{
          display: 'flex',
          alignItems: 'start',
          marginBottom: '1rem',
        }}
      >
        {cards.map((card, index) => {
          return (
            <div
              key={index}
              style={{
                padding: '0.5rem',
                width: '35%',
                minHeight: '22vh',
                marginRight:
                  card.title === 'Query Replication' ? '0.5rem' : 'auto',
                marginLeft:
                  card.title === 'Query Replication' ? '0.5rem' : 'auto',
                display: 'flex',
                flexDirection: 'column',
                justifyContent: 'space-between',
                border: '2px solid rgba(0, 0, 0, 0.07)',
                borderRadius: '1rem',
              }}
            >
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
                style={{ color: 'teal', cursor: 'pointer' }}
                href={card.link}
              >
                Learn more
              </Label>
            </div>
          );
        })}
      </div>
    </RadioButtonGroup>
  );
};

export default MirrorCards;
