import { FlowConnectionConfigs, QRepConfig } from '@/grpc_generated/flow';
import { Dispatch, SetStateAction } from 'react';

export type CDCConfig = FlowConnectionConfigs;
export type QREPConfig = QRepConfig;
export type MirrorConfig = CDCConfig | QREPConfig;
export type MirrorSetter = Dispatch<SetStateAction<CDCConfig | QREPConfig>>;
export type TableMapRow = { source: string; destination: string };
