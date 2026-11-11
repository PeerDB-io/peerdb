'use client';
import useHydrated from '@/app/utils/useHydrated';
import useLocalStorage from '@/app/utils/useLocalStorage';
import { MirrorStatusResponse } from '@/grpc_generated/route';
import { useTheme } from '@/lib/AppTheme';
import { Label } from '@/lib/Label';
import { ProgressCircle } from '@/lib/ProgressCircle';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@radix-ui/react-tabs';
import styled from 'styled-components';
import CdcDetails, { CdcFacts } from './cdcDetails';
import { SnapshotStatusTable } from './snapshot';
import { TabListStyle, TabsRootStyle } from './styles/tab.styles';

const StyledTabTrigger = styled(TabsTrigger)`
  background: none;
  border: 0;
  color: inherit;
  font: inherit;
  padding: 8px 0;
  cursor: pointer;
  &[data-state='active'] {
    font-weight: 500;
    box-shadow: inset 0 -2px 0 currentColor;
  }
`;

type CDCMirrorStatusProps = {
  status: MirrorStatusResponse;
  syncStatusChild?: React.ReactNode;
};
export function CDCMirror({ status, syncStatusChild }: CDCMirrorStatusProps) {
  const LocalStorageTabKey = `cdctab:${status.flowJobName}`;
  const theme = useTheme();
  const hydrated = useHydrated();
  const [selectedTab, setSelectedTab] = useLocalStorage(LocalStorageTabKey, 0);
  const handleTab = (index: number) => {
    setSelectedTab(index);
  };

  let snapshot = null;
  if (status.cdcStatus?.snapshotStatus) {
    snapshot = (
      <SnapshotStatusTable status={status.cdcStatus?.snapshotStatus} />
    );
  }

  if (!hydrated) {
    return (
      <div style={{ marginTop: '1rem' }}>
        <Label>
          <ProgressCircle variant='determinate_progress_circle' />
        </Label>
      </div>
    );
  }

  return (
    <>
      <CdcFacts mirrorConfig={status.cdcStatus!} />
      <Tabs
      style={TabsRootStyle}
      className='TabsRoot'
      value={selectedTab.toString()}
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
    </>
  );
}
