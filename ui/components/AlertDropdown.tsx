import { Button } from '@/lib/Button/Button';
import { Icon } from '@/lib/Icon';
import * as DropdownMenu from '@radix-ui/react-dropdown-menu';
import { useState } from 'react';
import DropDialog from './DropDialog';
const AlertDropdown = ({
  disable,
  alertId,
  onEdit,
}: {
  disable: boolean;
  alertId: number;
  onEdit: () => void;
}) => {
  const [open, setOpen] = useState(false);
  const handleToggle = () => {
    setOpen((prevOpen) => !prevOpen);
  };

  return (
    <DropdownMenu.Root>
      <DropdownMenu.Trigger>
        <Button
          aria-controls={open ? 'menu-list-grow' : undefined}
          aria-haspopup='true'
          onClick={handleToggle}
        >
          <Icon name='menu' />
        </Button>
      </DropdownMenu.Trigger>

      <DropdownMenu.Portal>
        <DropdownMenu.Content
          style={{
            border: '1px solid rgba(0,0,0,0.1)',
            borderRadius: '0.5rem',
          }}
        >
          <Button
            variant='normalBorderless'
            style={{ width: '100%', fontWeight: 'lighter' }}
            onClick={onEdit}
            disabled={disable}
          >
            Edit
          </Button>

          <DropDialog mode='ALERT' dropArgs={{ id: alertId }} />
        </DropdownMenu.Content>
      </DropdownMenu.Portal>
    </DropdownMenu.Root>
  );
};

export default AlertDropdown;
