'use client';
import { Button } from '@/lib/Button';
import { Icon } from '@/lib/Icon';
import { BarList } from '@tremor/react';
import { useState } from 'react';
const dataFormatter = (number: number) =>
  `${Intl.NumberFormat('us').format(number).toString()}`;

const RowsDisplay = ({
  totalRowsData,
}: {
  totalRowsData: {
    total: Number;
    inserts: number;
    updates: number;
    deletes: number;
  };
}) => {
  const [show, setShow] = useState(false);
  const rowsHero = [
    { name: 'Inserts', value: totalRowsData.inserts },
    { name: 'Updates', value: totalRowsData.updates, color: 'yellow' },
    { name: 'Deletes', value: totalRowsData.deletes, color: 'rose' },
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
        {dataFormatter(totalRowsData.total.valueOf())}
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
          <BarList valueFormatter={dataFormatter} data={rowsHero} />
        </div>
      )}
    </div>
  );
};

export default RowsDisplay;
