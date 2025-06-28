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
    <div className='mb-6'>
      <Label variant='headline' className='mb-4 block'>
        Maintenance Phases
      </Label>
      <div className='flex flex-col space-y-3'>
        {phases.map((phase) => {
          const isActive = normalizedCurrentPhase === phase;
          return (
            <div
              key={phase}
              className={`p-3 rounded-lg border transition-colors ${
                isActive
                  ? 'bg-green-100 border-green-300 text-green-800'
                  : 'bg-gray-50 border-gray-200 text-gray-600'
              }`}
            >
              <div className='flex items-center justify-between'>
                <span className='font-medium'>{phaseDisplayNames[phase]}</span>
                {isActive && (
                  <Icon name='check_circle' className='text-green-600' />
                )}
              </div>
            </div>
          );
        })}
      </div>
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
      <div className='text-center py-8 text-gray-500'>
        <Icon name='check_circle' className='text-4xl mb-2' />
        <p>No pending activities</p>
      </div>
    );
  }

  // Sort activities by name
  const sortedActivities = [...activities].sort((a, b) =>
    (a.activityName || '').localeCompare(b.activityName || '')
  );

  return (
    <div className='overflow-x-auto'>
      <table className='w-full border-collapse border border-gray-300'>
        <thead className='bg-gray-50'>
          <tr>
            <th
              className='border border-gray-300 px-4 py-2 text-left'
              style={{ width: '30%' }}
            >
              Activity Name
            </th>
            <th
              className='border border-gray-300 px-4 py-2 text-left'
              style={{ width: '10%' }}
            >
              Duration
            </th>
            <th
              className='border border-gray-300 px-4 py-2 text-left'
              style={{ width: '60%' }}
            >
              Last Heartbeat Payload
            </th>
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
              <tr key={index} className='hover:bg-gray-50'>
                <td
                  className='border border-gray-300 px-4 py-2 font-mono text-sm'
                  style={{ width: '30%' }}
                >
                  {activity.activityName}
                </td>
                <td
                  className='border border-gray-300 px-4 py-2'
                  style={{ width: '10%' }}
                >
                  {formatDuration(activity.activityDuration)}
                </td>
                <td
                  className='border border-gray-300 px-4 py-2'
                  style={{ width: '60%' }}
                >
                  {lastPayload ? (
                    <div
                      className='text-sm font-mono bg-gray-100 p-2 rounded truncate'
                      title={lastPayload}
                    >
                      {lastPayload.length > 100
                        ? `${lastPayload.substring(0, 100)}...`
                        : lastPayload}
                    </div>
                  ) : (
                    <span className='text-gray-500 italic'>No payload</span>
                  )}
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
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
        throw new Error(`HTTP error! status: ${response.status}`);
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
    <div className='mt-6'>
      <Label variant='headline' className='mb-4 block'>
        Skip Snapshot Wait for Flow
      </Label>
      <div className='bg-yellow-50 border border-yellow-200 rounded-lg p-4'>
        <div className='flex items-center gap-2 mb-3'>
          <Icon name='warning' className='text-yellow-600' />
          <Label className='text-yellow-800 font-medium'>
            Send signal to skip snapshot wait for a specific flow during
            maintenance startup
          </Label>
        </div>

        <div className='flex gap-3 items-start'>
          <div className='flex-1'>
            <input
              type='text'
              value={flowName}
              onChange={(e) => setFlowName(e.target.value)}
              placeholder='Enter flow name (e.g., mirror_name)'
              className='w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent'
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
                <Icon name='sync' className='animate-spin' />
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
          <div
            className={`mt-3 p-3 rounded-md ${
              skipResult.success
                ? 'bg-green-100 border border-green-200 text-green-800'
                : 'bg-red-100 border border-red-200 text-red-800'
            }`}
          >
            <div className='flex items-center gap-2'>
              <Icon name={skipResult.success ? 'check_circle' : 'error'} />
              <span className='text-sm font-medium'>{skipResult.message}</span>
            </div>
          </div>
        )}
      </div>
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
        <div className='p-6'>
          <Header variant='largeTitle'>Maintenance</Header>

          {isLoading && (
            <div className='flex items-center gap-2 mt-4'>
              <Icon name='sync' className='animate-spin' />
              <Label>Loading maintenance status...</Label>
            </div>
          )}

          {error && (
            <div className='flex items-center gap-2 mt-4 p-4 bg-red-100 border border-red-300 rounded-lg'>
              <Icon name='error' className='text-red-600' />
              <Label className='text-red-700'>
                Failed to load maintenance status
              </Label>
            </div>
          )}

          {maintenanceStatus && (
            <div className='mt-6'>
              {/* Maintenance Status */}
              <div className='mb-6'>
                <div
                  className={`flex items-center gap-2 p-4 rounded-lg border ${
                    maintenanceStatus.maintenanceRunning
                      ? 'bg-orange-100 border-orange-500'
                      : 'bg-green-100 border-green-500'
                  }`}
                >
                  <Icon
                    name={
                      maintenanceStatus.maintenanceRunning
                        ? 'build'
                        : 'check_circle'
                    }
                    className={
                      maintenanceStatus.maintenanceRunning
                        ? 'text-orange-600'
                        : 'text-green-600'
                    }
                  />
                  <Label className='text-lg font-semibold'>
                    Maintenance Status:{' '}
                    {maintenanceStatus.maintenanceRunning
                      ? 'RUNNING'
                      : 'NOT RUNNING'}
                  </Label>
                </div>
              </div>

              {/* Phase Indicator */}
              <PhaseIndicator currentPhase={maintenanceStatus.phase} />

              {/* Pending Activities */}
              <div>
                <Label variant='headline' className='mb-4 block'>
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
