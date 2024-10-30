'use client';
import { MirrorStatusResponse } from '@/grpc_generated/route';
import { Label } from '@/lib/Label';
import { ProgressCircle } from '@/lib/ProgressCircle';
import { Tab, TabGroup, TabList, TabPanel, TabPanels } from '@tremor/react';
import { useEffect, useState } from 'react';
import { useLocalStorage } from 'usehooks-ts';
import CdcDetails from './cdcDetails';
import { SnapshotStatusTable } from './snapshot';

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
  return (
    <TabGroup
      index={selectedTab}
      onIndexChange={handleTab}
      style={{ marginTop: '1rem' }}
    >
      <TabList
        color='neutral'
        style={{ display: 'flex', justifyContent: 'space-around' }}
      >
        <Tab>Overview</Tab>
        <Tab>Sync Status</Tab>
        <Tab>Initial Copy</Tab>
      </TabList>
      <TabPanels>
        <TabPanel>
          <CdcDetails
            createdAt={status.createdAt}
            mirrorConfig={status.cdcStatus!}
            mirrorStatus={status.currentFlowState}
          />
        </TabPanel>
        <TabPanel>{syncStatusChild}</TabPanel>
        <TabPanel>{snapshot}</TabPanel>
      </TabPanels>
    </TabGroup>
  );
}
