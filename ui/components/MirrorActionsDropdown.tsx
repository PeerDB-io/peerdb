'use client';
import CancelTableAdditionButton from '@/components/CancelTableAdditionButton';
import EditButton from '@/components/EditButton';
import ResyncDialog from '@/components/ResyncDialog';
import { FlowStatus } from '@/grpc_generated/flow';
import { Button } from '@/lib/Button';
import { Icon } from '@/lib/Icon';
import { CSSProperties, useState } from 'react';
import { useTheme as useStyledTheme } from 'styled-components';
import PauseOrResumeButton from './PauseOrResumeButton';

type MirrorActionsProps = {
  mirrorName: string;
  mirrorStatus: FlowStatus;
  editLink: string;
  canResync: boolean;
  isNotPaused: boolean;
};

export default function MirrorActions({
  mirrorName,
  mirrorStatus,
  editLink,
  canResync,
  isNotPaused,
}: MirrorActionsProps) {
  const theme = useStyledTheme();
  const [showOptions, setShowOptions] = useState(false);

  const menuStyle: CSSProperties = {
    display: 'flex',
    flexDirection: 'column',
    position: 'absolute',
    top: '50px',
    right: '0',
    backgroundColor: theme.colors.base.surface.normal,
    border: `1px solid ${theme.colors.base.border.normal}`,
    borderRadius: '5px',
    boxShadow: '0 0 5px rgba(0,0,0,0.2)',
  };

  return (
    <div
      style={{
        position: 'relative',
        display: 'flex',
        flexDirection: 'column',
        alignItems: 'center',
        width: '10rem',
      }}
    >
      <Button onClick={() => setShowOptions((v) => !v)} variant='normal'>
        Actions <Icon name='arrow_drop_down' />
      </Button>
      {showOptions && (
        <div style={menuStyle}>
          <PauseOrResumeButton
            mirrorName={mirrorName}
            mirrorStatus={mirrorStatus}
          />
          <EditButton toLink={editLink} disabled={isNotPaused} />
          {canResync && <ResyncDialog mirrorName={mirrorName} />}
          {(mirrorStatus === FlowStatus.STATUS_SETUP ||
            mirrorStatus === FlowStatus.STATUS_SNAPSHOT) && (
            <CancelTableAdditionButton mirrorName={mirrorName} />
          )}
        </div>
      )}
    </div>
  );
}
