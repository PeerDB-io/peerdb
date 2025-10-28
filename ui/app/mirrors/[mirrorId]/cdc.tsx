'use client';
import useLocalStorage from '@/app/utils/useLocalStorage';
import { MirrorStatusResponse } from '@/grpc_generated/route';
import { useTheme } from '@/lib/AppTheme';
import { Label } from '@/lib/Label';
import { ProgressCircle } from '@/lib/ProgressCircle';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@radix-ui/react-tabs';
import { useEffect, useState } from 'react';
import styled from 'styled-components';
import CdcDetails from './cdcDetails';
import { SnapshotStatusTable } from './snapshot';
import { TabListStyle, TabsRootStyle } from './styles/tab.styles';

const StyledTabTrigger = styled(TabsTrigger)`
  &[data-state='active'] {
    font-weight: bold;
  }
`;

type CDCMirrorStatusProps = {
  status: MirrorStatusResponse;
  syncStatusChild?: React.ReactNode;
};
export function CDCMirror({ status, syncStatusChild }: CDCMirrorStatusProps) {
  const LocalStorageTabKey = 'cdctab';
  const [selectedTab, setSelectedTab] = useLocalStorage(LocalStorageTabKey, 0);
  const [mounted, setMounted] = useState(false);
  const handleTab = (index: number) => {
    setSelectedTab(index);
  };

  let snapshot = null;
  if (status.cdcStatus?.snapshotStatus) {
    snapshot = (
      <SnapshotStatusTable status={status.cdcStatus?.snapshotStatus} />
    );
  }
  useEffect(() => {
    setMounted(true);
  }, []);
  if (!mounted) {
    return (
      <div style={{ marginTop: '1rem' }}>
        <Label>
          <ProgressCircle variant='determinate_progress_circle' />
        </Label>
      </div>
    );
  }

  const theme = useTheme();
  return (
    <Tabs
      style={TabsRootStyle}
      className='TabsRoot'
      defaultValue={selectedTab.toString()}
      onValueChange={(value) => handleTab(Number(value))}
    >
      <TabsList style={TabListStyle(theme.theme)} className='TabsList'>
        <StyledTabTrigger className='TabsTrigger' value='0'>
          Overview
        </StyledTabTrigger>
        <StyledTabTrigger value='1'>Sync Status</StyledTabTrigger>
        <StyledTabTrigger value='2'>Initial Copy</StyledTabTrigger>
      </TabsList>
      <TabsContent className='TabsContent' value='0'>
        <CdcDetails
          createdAt={status.createdAt}
          mirrorConfig={status.cdcStatus!}
          mirrorStatus={status.currentFlowState}
        />
      </TabsContent>
      <TabsContent className='TabsContent' value='1'>
        {syncStatusChild}
      </TabsContent>
      <TabsContent className='TabsContent' value='2'>
        {snapshot}
      </TabsContent>
    </Tabs>
  );
}
