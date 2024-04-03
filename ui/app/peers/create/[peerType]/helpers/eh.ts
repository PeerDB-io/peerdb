import { EventHubConfig, EventHubGroupConfig } from '@/grpc_generated/peers';
import { PeerSetting } from './common';

export const ehSetting: PeerSetting[] = [
  {
    label: 'Namespace',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, namespace: value as string })),
    tips: 'An Event Hubs namespace is a management container for event hubs (or topics)',
    helpfulLink:
      'https://learn.microsoft.com/en-us/azure/event-hubs/event-hubs-features#namespace',
  },
  {
    label: 'Subscription ID',
    type: 'password',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, subscriptionId: value as string })),
    tips: 'Subscription ID uniquely identify a Microsoft Azure subscription.',
  },
  {
    label: 'Resource Group',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, resourceGroup: value as string })),
    tips: 'A resource group is a logical collection of Azure resources.',
    helpfulLink:
      'https://learn.microsoft.com/en-us/azure/event-hubs/event-hubs-create#create-a-resource-group',
  },
  {
    label: 'Location',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, location: value as string })),
    tips: 'The location of the resource group',
    helpfulLink:
      'https://learn.microsoft.com/en-us/azure/reliability/availability-zones-service-support#azure-regions-with-availability-zones',
  },
  {
    label: 'Partition Count',
    stateHandler: (value, setter) =>
      setter((curr) => ({
        ...curr,
        partitionCount: parseInt(value as string, 10) ?? 1,
      })),
    tips: 'Event Hubs organizes sequences of events sent to an event hub into one or more partitions',
    helpfulLink:
      'https://learn.microsoft.com/en-us/azure/event-hubs/event-hubs-features#partitions',
    default: 1,
    type: 'number',
  },
  {
    label: 'Message Retention (Days)',
    stateHandler: (value, setter) =>
      setter((curr) => ({
        ...curr,
        messageRetentionInDays: parseInt(value as string, 10) ?? 1,
      })),
    placeholder: 'Number of days to retain messages',
    tips: 'The retention period for events in the event hub',
    helpfulLink:
      'https://learn.microsoft.com/en-us/azure/event-hubs/event-hubs-features#event-retention',
    default: 1,
    type: 'number',
  },
];

export const blankEventhubSetting: EventHubConfig = {
  namespace: '',
  resourceGroup: '',
  location: '',
  subscriptionId: '',
  partitionCount: 1,
  messageRetentionInDays: 1,
};

export const blankEventHubGroupSetting: EventHubGroupConfig = {
  eventhubs: {},
  unnestColumns: [],
};
