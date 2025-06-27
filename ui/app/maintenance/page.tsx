'use client';

import { fetcher } from '@/app/utils/swr';
import SidebarComponent from '@/components/SidebarComponent';
import { Header } from '@/lib/Header';
import { Layout, LayoutMain } from '@/lib/Layout';
import { Label } from '@/lib/Label';
import { Icon } from '@/lib/Icon';
import { Button } from '@/lib/Button';
import useSWR from 'swr';
import { Configuration } from '@/app/config/config';
import {
  MaintenanceStatusResponse,
  MaintenanceActivityDetails,
  MaintenancePhase,
  maintenancePhaseFromJSON,
} from '@/grpc_generated/route';
import { Duration } from '@/grpc_generated/google/protobuf/duration';

const phaseDisplayNames: Record<MaintenancePhase, string> = {
  [MaintenancePhase.MAINTENANCE_PHASE_START_MAINTENANCE]: 'Start Maintenance',
  [MaintenancePhase.MAINTENANCE_PHASE_MAINTENANCE_MODE_ENABLED]: 'Maintenance Mode Enabled',
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
    totalSeconds = seconds + (nanos / 1000000000);
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
    <div className="mb-6">
      <Label variant="headline" className="mb-4 block">
        Maintenance Phases
      </Label>
      <div className="flex flex-col space-y-3">
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
              <div className="flex items-center justify-between">
                <span className="font-medium">{phaseDisplayNames[phase]}</span>
                {isActive && (
                  <Icon name="check_circle" className="text-green-600" />
                )}
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}

function PendingActivitiesTable({ activities }: { activities: MaintenanceActivityDetails[] }) {
  if (!activities || activities.length === 0) {
    return (
      <div className="text-center py-8 text-gray-500">
        <Icon name="check_circle" className="text-4xl mb-2" />
        <p>No pending activities</p>
      </div>
    );
  }

  // Sort activities by name
  const sortedActivities = [...activities].sort((a, b) => 
    (a.activityName || '').localeCompare(b.activityName || '')
  );

  return (
    <div className="overflow-x-auto">
      <table className="w-full border-collapse border border-gray-300">
        <thead className="bg-gray-50">
          <tr>
            <th className="border border-gray-300 px-4 py-2 text-left">Activity Name</th>
            <th className="border border-gray-300 px-4 py-2 text-left">Duration</th>
            <th className="border border-gray-300 px-4 py-2 text-left">Last Heartbeat</th>
            <th className="border border-gray-300 px-4 py-2 text-left">Heartbeat Count</th>
            <th className="border border-gray-300 px-4 py-2 text-left">Last Heartbeat Payload</th>
          </tr>
        </thead>
        <tbody>
          {sortedActivities.map((activity, index) => {
            const lastPayload = activity.heartbeatPayloads && activity.heartbeatPayloads.length > 0 
              ? activity.heartbeatPayloads[activity.heartbeatPayloads.length - 1]
              : null;
              
            return (
              <tr key={index} className="hover:bg-gray-50">
                <td className="border border-gray-300 px-4 py-2 font-mono text-sm">
                  {activity.activityName}
                </td>
                <td className="border border-gray-300 px-4 py-2">
                  {formatDuration(activity.activityDuration)}
                </td>
                <td className="border border-gray-300 px-4 py-2">
                  {activity.lastHeartbeat ? 
                    new Date(activity.lastHeartbeat).toLocaleString() : 
                    'N/A'
                  }
                </td>
                <td className="border border-gray-300 px-4 py-2">
                  {activity.heartbeatPayloads?.length || 0}
                </td>
                <td className="border border-gray-300 px-4 py-2 max-w-xs">
                  {lastPayload ? (
                    <div className="text-sm font-mono bg-gray-100 p-2 rounded truncate" title={lastPayload}>
                      {lastPayload.length > 100 ? `${lastPayload.substring(0, 100)}...` : lastPayload}
                    </div>
                  ) : (
                    <span className="text-gray-500 italic">No payload</span>
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
      <LayoutMain alignSelf="flex-start" justifySelf="flex-start" width="full">
        <div className="p-6">
          <Header variant="largeTitle">Maintenance</Header>
          
          {isLoading && (
            <div className="flex items-center gap-2 mt-4">
              <Icon name="sync" className="animate-spin" />
              <Label>Loading maintenance status...</Label>
            </div>
          )}

          {error && (
            <div className="flex items-center gap-2 mt-4 p-4 bg-red-100 border border-red-300 rounded-lg">
              <Icon name="error" className="text-red-600" />
              <Label className="text-red-700">Failed to load maintenance status</Label>
            </div>
          )}

          {maintenanceStatus && (
            <div className="mt-6">
              {/* Maintenance Status */}
              <div className="mb-6">
                <div className={`flex items-center gap-2 p-4 rounded-lg border ${
                  maintenanceStatus.maintenanceRunning
                    ? 'bg-orange-100 border-orange-500'
                    : 'bg-green-100 border-green-500'
                }`}>
                  <Icon 
                    name={maintenanceStatus.maintenanceRunning ? 'build' : 'check_circle'}
                    className={maintenanceStatus.maintenanceRunning ? 'text-orange-600' : 'text-green-600'}
                  />
                  <Label className="text-lg font-semibold">
                    Maintenance Status: {maintenanceStatus.maintenanceRunning ? 'RUNNING' : 'NOT RUNNING'}
                  </Label>
                </div>
              </div>

              {/* Phase Indicator */}
              <PhaseIndicator currentPhase={maintenanceStatus.phase} />

              {/* Pending Activities */}
              <div>
                <Label variant="headline" className="mb-4 block">
                  Pending Activities ({maintenanceStatus.pendingActivities?.length || 0})
                </Label>
                <PendingActivitiesTable activities={maintenanceStatus.pendingActivities || []} />
              </div>
            </div>
          )}
        </div>
      </LayoutMain>
    </Layout>
  );
}
