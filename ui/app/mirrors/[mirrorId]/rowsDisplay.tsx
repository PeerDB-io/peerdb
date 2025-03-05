'use client';
import { CDCRowCounts } from '@/grpc_generated/route';
import { Button } from '@/lib/Button';
import { Icon } from '@/lib/Icon';
import { BarList } from '@tremor/react';
import { useState } from 'react';

export function RowDataFormatter(number: number) {
  return `${Intl.NumberFormat('en-US').format(number).toString()}`;
}

export default function RowsDisplay({
  totalRowsData,
}: {
  totalRowsData: CDCRowCounts;
}) {
  const [show, setShow] = useState(false);
  const rowsHero = [
    { name: 'Inserts', value: totalRowsData.insertsCount },
    { name: 'Updates', value: totalRowsData.updatesCount, color: 'yellow' },
    { name: 'Deletes', value: totalRowsData.deletesCount, color: 'rose' },
  ];
  return (
    <div
      style={{
        marginTop: '2rem',
        display: 'flex',
        flexDirection: 'column',
        alignItems: 'center',
      }}
    >
      <h4 className='text-tremor-default text-tremor-content dark:text-dark-tremor-content'>
        Rows Synced
      </h4>
      <p className='text-tremor-metric text-tremor-content-strong dark:text-dark-tremor-content-strong font-semibold'>
        {RowDataFormatter(totalRowsData.totalCount.valueOf())}
      </p>
      <Button
        aria-label='icon-button'
        onClick={() => setShow((prev) => !prev)}
        variant='normal'
      >
        <Icon name={show ? 'arrow_drop_up' : 'arrow_drop_down'} />
      </Button>
      {show && (
        <div style={{ width: '30%', marginTop: '1.5rem' }}>
          <BarList valueFormatter={RowDataFormatter} data={rowsHero} />
        </div>
      )}
    </div>
  );
}
