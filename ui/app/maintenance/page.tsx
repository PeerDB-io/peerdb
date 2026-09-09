'use client';

import { Configuration } from '@/app/config/config';
import { fetcher } from '@/app/utils/swr';
import SidebarComponent from '@/components/SidebarComponent';
import { Duration } from '@/grpc_generated/google/protobuf/duration';
import {
  MaintenanceActivityDetails,
  MaintenancePhase,
  maintenancePhaseFromJSON,
  MaintenanceStatusResponse,
  SkipSnapshotWaitFlowsRequest,
  SkipSnapshotWaitFlowsResponse,
} from '@/grpc_generated/route';
import { Button } from '@/lib/Button';
import { Header } from '@/lib/Header';
import { Icon } from '@/lib/Icon';
import { Label } from '@/lib/Label';
import { Layout, LayoutMain } from '@/lib/Layout';
import { useState } from 'react';
import useSWR from 'swr';
import {
  ActivitiesTable,
  EmptyActivitiesIcon,
  FlowNameInput,
  HeartbeatPayload,
  PhaseCard,
  PhaseList,
  SignalResult,
  SkipWaitPanel,
  SpinningIcon,
  StatusBanner,
  StatusError,
  StatusIcon,
  StatusRow,
} from './styles';

const phaseDisplayNames: Record<MaintenancePhase, string> = {
  [MaintenancePhase.MAINTENANCE_PHASE_START_MAINTENANCE]: 'Start Maintenance',
  [MaintenancePhase.MAINTENANCE_PHASE_MAINTENANCE_MODE_ENABLED]:
    'Maintenance Mode Enabled',
  [MaintenancePhase.MAINTENANCE_PHASE_END_MAINTENANCE]: 'End Maintenance',
  [MaintenancePhase.MAINTENANCE_PHASE_UNKNOWN]: 'Unknown',
  [MaintenancePhase.UNRECOGNIZED]: 'Unrecognized',
};

function formatDuration(duration: Duration | string | undefined): string {
  if (!duration) {
    return 'N/A';
  }

  let totalSeconds: number;

  if (typeof duration === 'string') {
    // Handle string format like "92.789060820s"
    const match = duration.match(/^([0-9.]+)s?$/);
    if (match) {
      totalSeconds = parseFloat(match[1]);
    } else {
      return 'Invalid duration';
    }
  } else {
    // Handle protobuf Duration object format
    const seconds = duration?.seconds || 0;
    const nanos = duration?.nanos || 0;
    totalSeconds = seconds + nanos / 1000000000;
  }

  if (totalSeconds === 0) {
    return '0s';
  }

  const hours = Math.floor(totalSeconds / 3600);
  const minutes = Math.floor((totalSeconds % 3600) / 60);
  const remainingSeconds = totalSeconds % 60;

  const parts = [];
  if (hours > 0) parts.push(`${hours}h`);
  if (minutes > 0) parts.push(`${minutes}m`);
  if (remainingSeconds > 0 || (hours === 0 && minutes === 0)) {
    if (remainingSeconds < 60 && remainingSeconds % 1 !== 0) {
      parts.push(`${remainingSeconds.toFixed(3)}s`);
    } else {
      parts.push(`${Math.floor(remainingSeconds)}s`);
    }
  }

  return parts.join(' ');
}

function PhaseIndicator({ currentPhase }: { currentPhase: any }) {
  const phases = [
    MaintenancePhase.MAINTENANCE_PHASE_START_MAINTENANCE,
    MaintenancePhase.MAINTENANCE_PHASE_MAINTENANCE_MODE_ENABLED,
    MaintenancePhase.MAINTENANCE_PHASE_END_MAINTENANCE,
  ];

  // Use generated protobuf conversion function
  const normalizedCurrentPhase = maintenancePhaseFromJSON(currentPhase);

  return (
    <div style={{ marginBottom: '1.5rem' }}>
      <Label variant='headline'>Maintenance Phases</Label>
      <PhaseList>
        {phases.map((phase) => {
          const isActive = normalizedCurrentPhase === phase;
          return (
            <PhaseCard key={phase} $active={isActive}>
              <StatusRow style={{ justifyContent: 'space-between', gap: 0 }}>
                <span style={{ fontWeight: 500 }}>
                  {phaseDisplayNames[phase]}
                </span>
                {isActive && (
                  <StatusIcon name='check_circle' $status='success' />
                )}
              </StatusRow>
            </PhaseCard>
          );
        })}
      </PhaseList>
    </div>
  );
}

function PendingActivitiesTable({
  activities,
}: {
  activities: MaintenanceActivityDetails[];
}) {
  if (!activities || activities.length === 0) {
    return (
      <div
        style={{ textAlign: 'center', paddingBlock: '2rem', color: '#e2e2e2' }}
      >
        <EmptyActivitiesIcon name='check_circle' />
        <p>No pending activities</p>
      </div>
    );
  }

  // Sort activities by name
  const sortedActivities = [...activities].sort((a, b) =>
    (a.activityName || '').localeCompare(b.activityName || '')
  );

  return (
    <div style={{ overflowX: 'auto' }}>
      <ActivitiesTable>
        <thead>
          <tr>
            <th style={{ width: '30%' }}>Activity Name</th>
            <th style={{ width: '10%' }}>Duration</th>
            <th style={{ width: '60%' }}>Last Heartbeat Payload</th>
          </tr>
        </thead>
        <tbody>
          {sortedActivities.map((activity, index) => {
            const lastPayload =
              activity.heartbeatPayloads &&
              activity.heartbeatPayloads.length > 0
                ? activity.heartbeatPayloads[
                    activity.heartbeatPayloads.length - 1
                  ]
                : null;

            return (
              <tr key={index}>
                <td style={{ width: '30%' }}>{activity.activityName}</td>
                <td style={{ width: '10%' }}>
                  {formatDuration(activity.activityDuration)}
                </td>
                <td style={{ width: '60%' }}>
                  {lastPayload ? (
                    <HeartbeatPayload title={lastPayload}>
                      {lastPayload.length > 100
                        ? `${lastPayload.substring(0, 100)}...`
                        : lastPayload}
                    </HeartbeatPayload>
                  ) : (
                    <span style={{ color: '#e2e2e2', fontStyle: 'italic' }}>
                      No payload
                    </span>
                  )}
                </td>
              </tr>
            );
          })}
        </tbody>
      </ActivitiesTable>
    </div>
  );
}

function SkipSnapshotWaitSection() {
  const [flowName, setFlowName] = useState('');
  const [isSkipping, setIsSkipping] = useState(false);
  const [skipResult, setSkipResult] = useState<{
    success: boolean;
    message: string;
  } | null>(null);

  const handleSkipSnapshotWait = async () => {
    if (!flowName.trim()) {
      setSkipResult({ success: false, message: 'Please enter a flow name' });
      return;
    }

    setIsSkipping(true);
    setSkipResult(null);

    try {
      const request: SkipSnapshotWaitFlowsRequest = {
        flowNames: [flowName.trim()],
      };

      const response = await fetch(
        '/api/v1/instance/maintenance/skip-snapshot-wait',
        {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
          },
          body: JSON.stringify(request),
        }
      );

      if (!response.ok) {
        throw new Error(
          `HTTP error! status: ${response.status}, body: ${await response.text()}`
        );
      }

      const result: SkipSnapshotWaitFlowsResponse = await response.json();
      setSkipResult({
        success: result.signalSent,
        message:
          result.message ||
          (result.signalSent
            ? 'Signal sent successfully'
            : 'Failed to send signal'),
      });

      if (result.signalSent) {
        setFlowName(''); // Clear input on success
      }
    } catch (error) {
      setSkipResult({
        success: false,
        message: `Error: ${error instanceof Error ? error.message : 'Unknown error'}`,
      });
    } finally {
      setIsSkipping(false);
    }
  };

  return (
    <div style={{ marginTop: '1.5rem' }}>
      <Label variant='headline'>Skip Snapshot Wait for Flow</Label>
      <SkipWaitPanel>
        <StatusRow style={{ marginBottom: '0.75rem' }}>
          <StatusIcon name='warning' $status='warning' />
          <Label>
            Send signal to skip snapshot wait for a specific flow during
            maintenance startup
          </Label>
        </StatusRow>

        <div
          style={{ display: 'flex', gap: '0.75rem', alignItems: 'flex-start' }}
        >
          <div style={{ flex: 1 }}>
            <FlowNameInput
              type='text'
              value={flowName}
              onChange={(e) => setFlowName(e.target.value)}
              placeholder='Enter mirror name (e.g., mirror_name)'
              disabled={isSkipping}
            />
          </div>
          <Button
            variant='normal'
            onClick={handleSkipSnapshotWait}
            disabled={isSkipping || !flowName.trim()}
          >
            {isSkipping ? (
              <>
                <SpinningIcon name='sync' />
                Sending...
              </>
            ) : (
              <>
                <Icon name='skip_next' />
                Skip Wait
              </>
            )}
          </Button>
        </div>

        {skipResult && (
          <SignalResult $success={skipResult.success}>
            <StatusRow>
              <Icon name={skipResult.success ? 'check_circle' : 'error'} />
              <span
                style={{
                  fontSize: '0.875rem',
                  lineHeight: '1.25rem',
                  fontWeight: 500,
                }}
              >
                {skipResult.message}
              </span>
            </StatusRow>
          </SignalResult>
        )}
      </SkipWaitPanel>
    </div>
  );
}

export default function MaintenancePage() {
  const {
    data: maintenanceStatus,
    error,
    isLoading,
  }: {
    data: MaintenanceStatusResponse;
    error: any;
    isLoading: boolean;
  } = useSWR('/api/v1/instance/maintenance/status', fetcher, {
    refreshInterval: 2000, // Refresh every 2 seconds for real-time updates
  });

  return (
    <Layout
      sidebar={
        <SidebarComponent
          showLogout={!!Configuration.authentication.PEERDB_PASSWORD}
        />
      }
    >
      <LayoutMain alignSelf='flex-start' justifySelf='flex-start' width='full'>
        <div style={{ padding: '1.5rem' }}>
          <Header variant='largeTitle'>Maintenance</Header>

          {isLoading && (
            <StatusRow style={{ marginTop: '1rem' }}>
              <SpinningIcon name='sync' />
              <Label>Loading maintenance status...</Label>
            </StatusRow>
          )}

          {error && (
            <StatusError>
              <StatusIcon name='error' $status='error' />
              <Label>Failed to load maintenance status</Label>
            </StatusError>
          )}

          {maintenanceStatus && (
            <div style={{ marginTop: '1.5rem' }}>
              {/* Maintenance Status */}
              <div style={{ marginBottom: '1.5rem' }}>
                <StatusBanner $running={maintenanceStatus.maintenanceRunning}>
                  <StatusIcon
                    name={
                      maintenanceStatus.maintenanceRunning
                        ? 'build'
                        : 'check_circle'
                    }
                    $status={
                      maintenanceStatus.maintenanceRunning
                        ? 'running'
                        : 'success'
                    }
                  />
                  <Label>
                    Maintenance Status:{' '}
                    {maintenanceStatus.maintenanceRunning
                      ? 'RUNNING'
                      : 'NOT RUNNING'}
                  </Label>
                </StatusBanner>
              </div>

              {/* Phase Indicator */}
              <PhaseIndicator currentPhase={maintenanceStatus.phase} />

              {/* Pending Activities */}
              <div>
                <Label variant='headline'>
                  Pending Activities (
                  {maintenanceStatus.pendingActivities?.length || 0})
                </Label>
                <PendingActivitiesTable
                  activities={maintenanceStatus.pendingActivities || []}
                />
              </div>

              {/* Skip Snapshot Wait - Only show during StartMaintenance phase */}
              {maintenancePhaseFromJSON(maintenanceStatus.phase) ===
                MaintenancePhase.MAINTENANCE_PHASE_START_MAINTENANCE && (
                <SkipSnapshotWaitSection />
              )}
            </div>
          )}
        </div>
      </LayoutMain>
    </Layout>
  );
}
