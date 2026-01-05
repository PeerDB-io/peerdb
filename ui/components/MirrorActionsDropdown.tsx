'use client';
import { getMirrorState } from '@/app/mirrors/[mirrorId]/handlers';
import EditButton from '@/components/EditButton';
import ResyncDialog from '@/components/ResyncDialog';
import { FlowStatus } from '@/grpc_generated/flow';
import { Button } from '@/lib/Button';
import { Icon } from '@/lib/Icon';
import { useEffect, useState } from 'react';
import { CSSProperties } from 'react';
import { useTheme as useStyledTheme } from 'styled-components';
import PauseOrResumeButton from './PauseOrResumeButton';

type MirrorActionsProps = {
  mirrorName: string;
  editLink: string;
  canResync: boolean;
  isNotPaused: boolean;
};

export default function MirrorActions({
  mirrorName,
  editLink,
  canResync,
  isNotPaused,
}: MirrorActionsProps) {
  const theme = useStyledTheme();
  const [mirrorStatus, setMirrorStatus] = useState<FlowStatus>();
  const [mounted, setMounted] = useState(false);

  const [showOptions, setShowOptions] = useState(false);
  const handleButtonClick = () => setShowOptions(!showOptions);

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

  useEffect(() => {
    const fetchMirrorState = async () => {
      try {
        const res = await getMirrorState(mirrorName);
        setMirrorStatus(res.currentFlowState);
        setMounted(true);
      } catch (error) {
        console.error('Error fetching mirror state:', error);
        setMounted(true); // Still set mounted even on error
      }
    };

    fetchMirrorState();
  }, [mirrorName]);

  return (
    mounted && (
      <div
        style={{
          position: 'relative',
          display: 'flex',
          flexDirection: 'column',
          alignItems: 'center',
          width: '10rem',
        }}
      >
        <Button onClick={handleButtonClick} variant='normal'>
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
    )
  );
}
