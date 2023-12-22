'use client';
import { Button } from '@/lib/Button';
import { Dialog, DialogClose } from '@/lib/Dialog';
import { Icon } from '@/lib/Icon';
import { Label } from '@/lib/Label';

interface InfoPopoverProps {
  configs: {
    label: string;
    value?: string | number;
  }[];
}

const MirrorInfo = ({ configs }: InfoPopoverProps) => {
  return (
    <Dialog
      noInteract={false}
      size='large'
      triggerButton={
        <Button style={{ backgroundColor: 'white', border: '1px solid gray' }}>
          <Label variant='body'>View More</Label>
        </Button>
      }
    >
      <div
        style={{ display: 'flex', flexDirection: 'column', padding: '1rem' }}
      >
        <div
          style={{
            display: 'flex',
            justifyContent: 'space-between',
            marginBottom: '0.5rem',
          }}
        >
          <Label variant='headline'>Configuration</Label>
          <DialogClose>
            <button className='IconButton' aria-label='Close'>
              <Icon name='close' />
            </button>
          </DialogClose>
        </div>
        <table>
          <tbody>
            {configs.map((config, index) => (
              <tr key={index}>
                <td>
                  <Label as='label' style={{ fontSize: 15 }}>
                    {config.label}
                  </Label>
                </td>
                <td>
                  <Label as='label' style={{ fontSize: 15 }}>
                    {config.value}
                  </Label>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </Dialog>
  );
};

export default MirrorInfo;
