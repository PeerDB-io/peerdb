'use client';
import useHydrated from '@/app/utils/useHydrated';
import useLocalStorage from '@/app/utils/useLocalStorage';
import { DBType } from '@/grpc_generated/peers';
import { MirrorStatusResponse } from '@/grpc_generated/route';
import { useTheme } from '@/lib/AppTheme';
import { Label } from '@/lib/Label';
import { ProgressCircle } from '@/lib/ProgressCircle';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@radix-ui/react-tabs';
import styled from 'styled-components';
import CdcDetails from './cdcDetails';
import { SnapshotStatusTable } from './snapshot';
import { TabListStyle, TabsRootStyle } from './styles/tab.styles';
import TableReplicationState from './tableReplicationState';

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

  const isBigQuerySource =
    status.cdcStatus?.sourceType?.toString() ===
    DBType[DBType.BIGQUERY].toString();

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
        {isBigQuerySource && (
          <StyledTabTrigger value='3'>Table Replication State</StyledTabTrigger>
        )}
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
      {isBigQuerySource && (
        <TabsContent className='TabsContent' value='3'>
          <TableReplicationState flowJobName={status.flowJobName} />
        </TabsContent>
      )}
    </Tabs>
  );
}
