'use client';
import EditButton from '@/components/EditButton';
import { ResyncDialog } from '@/components/ResyncDialog';
import { FlowConnectionConfigs } from '@/grpc_generated/flow';
import { Button } from '@/lib/Button/Button';
import { Icon } from '@/lib/Icon';
import { Label } from '@/lib/Label/Label';
import * as DropdownMenu from '@radix-ui/react-dropdown-menu';
import { useEffect, useState } from 'react';

const MirrorActions = ({
  mirrorConfig,
  workflowId,
  editLink,
  canResync,
  isNotPaused,
}: {
  mirrorConfig: FlowConnectionConfigs;
  workflowId: string;
  editLink: string;
  canResync: boolean;
  isNotPaused: boolean;
}) => {
  const [mounted, setMounted] = useState(false);
  const [open, setOpen] = useState(false);
  const handleToggle = () => {
    setOpen((prevOpen) => !prevOpen);
  };
  useEffect(() => setMounted(true), []);
  if (mounted)
    return (
      <DropdownMenu.Root>
        <DropdownMenu.Trigger>
          <Button
            aria-controls={open ? 'menu-list-grow' : undefined}
            aria-haspopup='true'
            variant='normal'
            onClick={handleToggle}
            style={{
              boxShadow: '0px 1px 1px rgba(0,0,0,0.1)',
              border: '1px solid rgba(0,0,0,0.1)',
            }}
          >
            <Label>Actions</Label>
            <Icon name='arrow_downward_alt' />
          </Button>
        </DropdownMenu.Trigger>

        <DropdownMenu.Portal>
          <DropdownMenu.Content
            style={{
              border: '1px solid rgba(0,0,0,0.1)',
              borderRadius: '0.5rem',
              backgroundColor: 'white',
            }}
          >
            <EditButton toLink={editLink} disabled={isNotPaused} />

            {canResync && (
              <ResyncDialog
                mirrorConfig={mirrorConfig}
                workflowId={workflowId}
              />
            )}
          </DropdownMenu.Content>
        </DropdownMenu.Portal>
      </DropdownMenu.Root>
    );
  return <></>;
};

export default MirrorActions;
