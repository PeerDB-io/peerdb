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

// Types for maintenance API response
interface MaintenanceActivityDetails {
  activityName: string;
  duration: string;
  lastHeartbeatTimestamp: string;
  heartbeatPayloads: string[];
}

interface MaintenanceStatusResponse {
  maintenanceRunning: boolean;
  phase: string;
  pendingActivities: MaintenanceActivityDetails[];
}

enum MaintenancePhase {
  MAINTENANCE_PHASE_UNKNOWN = 'MAINTENANCE_PHASE_UNKNOWN',
  MAINTENANCE_PHASE_START_MAINTENANCE = 'MAINTENANCE_PHASE_START_MAINTENANCE',
  MAINTENANCE_PHASE_MAINTENANCE_MODE_ENABLED = 'MAINTENANCE_PHASE_MAINTENANCE_MODE_ENABLED',
  MAINTENANCE_PHASE_END_MAINTENANCE = 'MAINTENANCE_PHASE_END_MAINTENANCE',
}

const phaseDisplayNames = {
  [MaintenancePhase.MAINTENANCE_PHASE_START_MAINTENANCE]: 'Start Maintenance',
  [MaintenancePhase.MAINTENANCE_PHASE_MAINTENANCE_MODE_ENABLED]: 'Maintenance Mode Enabled',
  [MaintenancePhase.MAINTENANCE_PHASE_END_MAINTENANCE]: 'End Maintenance',
  [MaintenancePhase.MAINTENANCE_PHASE_UNKNOWN]: 'Unknown',
};

function PhaseIndicator({ currentPhase }: { currentPhase: string }) {
  const phases = [
    MaintenancePhase.MAINTENANCE_PHASE_START_MAINTENANCE,
    MaintenancePhase.MAINTENANCE_PHASE_MAINTENANCE_MODE_ENABLED,
    MaintenancePhase.MAINTENANCE_PHASE_END_MAINTENANCE,
  ];

  return (
    <div className="flex flex-col gap-2 mb-6">
      <Label variant="headline">Maintenance Phases</Label>
      <div className="flex gap-4">
        {phases.map((phase) => (
          <div
            key={phase}
            className={`flex items-center gap-2 px-4 py-2 rounded-lg border ${
              currentPhase === phase
                ? 'bg-green-100 border-green-500 text-green-700'
                : 'bg-gray-100 border-gray-300 text-gray-600'
            }`}
          >
            <Icon
              name={currentPhase === phase ? 'check_circle' : 'radio_button_unchecked'}
              className={currentPhase === phase ? 'text-green-600' : 'text-gray-400'}
            />
            <Label className={currentPhase === phase ? 'font-semibold' : ''}>
              {phaseDisplayNames[phase]}
            </Label>
          </div>
        ))}
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

  return (
    <div className="overflow-x-auto">
      <table className="w-full border-collapse border border-gray-300">
        <thead className="bg-gray-50">
          <tr>
            <th className="border border-gray-300 px-4 py-2 text-left">Activity Name</th>
            <th className="border border-gray-300 px-4 py-2 text-left">Duration</th>
            <th className="border border-gray-300 px-4 py-2 text-left">Last Heartbeat</th>
            <th className="border border-gray-300 px-4 py-2 text-left">Heartbeat Count</th>
            <th className="border border-gray-300 px-4 py-2 text-left">Actions</th>
          </tr>
        </thead>
        <tbody>
          {activities.map((activity, index) => (
            <tr key={index} className="hover:bg-gray-50">
              <td className="border border-gray-300 px-4 py-2 font-mono text-sm">
                {activity.activityName}
              </td>
              <td className="border border-gray-300 px-4 py-2">
                {activity.duration}
              </td>
              <td className="border border-gray-300 px-4 py-2">
                {activity.lastHeartbeatTimestamp ? 
                  new Date(activity.lastHeartbeatTimestamp).toLocaleString() : 
                  'N/A'
                }
              </td>
              <td className="border border-gray-300 px-4 py-2">
                {activity.heartbeatPayloads?.length || 0}
              </td>
              <td className="border border-gray-300 px-4 py-2">
                {activity.heartbeatPayloads && activity.heartbeatPayloads.length > 0 && (
                  <Button
                    variant="normalBorderless"
                    onClick={() => {
                      // Show heartbeat payloads in a modal or expandable section
                      console.log('Heartbeat payloads:', activity.heartbeatPayloads);
                      alert(`Heartbeat payloads:\n${activity.heartbeatPayloads.join('\n\n')}`);
                    }}
                  >
                    <Icon name="visibility" />
                    View Heartbeats
                  </Button>
                )}
              </td>
            </tr>
          ))}
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
