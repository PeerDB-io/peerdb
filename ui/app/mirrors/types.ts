import { FlowConnectionConfigs, QRepConfig } from '@/grpc_generated/flow';
import { Dispatch, SetStateAction } from 'react';

export type CDCConfig = FlowConnectionConfigs;
export type MirrorConfig = CDCConfig | QRepConfig;
export type MirrorSetter = Dispatch<SetStateAction<CDCConfig | QRepConfig>>;
export type TableMapRow = { source: string; destination: string };
