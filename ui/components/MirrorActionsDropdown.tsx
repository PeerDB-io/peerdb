'use client';
import { getMirrorState } from '@/app/mirrors/[mirrorId]/handlers';
import EditButton from '@/components/EditButton';
import { ResyncDialog } from '@/components/ResyncDialog';
import { FlowStatus } from '@/grpc_generated/flow';
import { MirrorStatusResponse } from '@/grpc_generated/route';
import { Button } from '@/lib/Button';
import { Icon } from '@/lib/Icon';
import { useEffect, useState } from 'react';
import { CSSProperties } from 'styled-components';
import PauseOrResumeButton from './PauseOrResumeButton';

type MirrorActionsProps = {
  mirrorName: string;
  editLink: string;
  canResync: boolean;
  isNotPaused: boolean;
};

const menuStyle: CSSProperties = {
  display: 'flex',
  flexDirection: 'column',
  position: 'absolute',
  top: '50px',
  right: '0',
  backgroundColor: 'white',
  border: '1px solid #ccc',
  borderRadius: '5px',
  boxShadow: '0 0 5px rgba(0,0,0,0.2)',
};

const MirrorActions = ({
  mirrorName,
  editLink,
  canResync,
  isNotPaused,
}: MirrorActionsProps) => {
  const [mirrorStatus, setMirrorStatus] = useState<FlowStatus>();
  const [mounted, setMounted] = useState(false);

  const [showOptions, setShowOptions] = useState(false);
  const handleButtonClick = () => setShowOptions(!showOptions);

  useEffect(() => {
    getMirrorState(mirrorName).then((res: MirrorStatusResponse) => {
      setMirrorStatus(res.currentFlowState);
    });
    setMounted(true);
  }, [mirrorName]);

  if (mounted)
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
        <Button
          onClick={handleButtonClick}
          style={{ backgroundColor: '#F8F8F8' }}
        >
          Actions <Icon name='arrow_drop_down' />
        </Button>
        {showOptions && (
          <div style={menuStyle}>
            {mirrorStatus && (
              <PauseOrResumeButton
                mirrorName={mirrorName}
                mirrorStatus={mirrorStatus}
              />
            )}
            <EditButton toLink={editLink} disabled={isNotPaused} />
            {canResync && <ResyncDialog mirrorName={mirrorName} />}
          </div>
        )}
      </div>
    );
  return <></>;
};

export default MirrorActions;
