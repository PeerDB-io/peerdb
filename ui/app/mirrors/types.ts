import { FlowConnectionConfigs } from '@/grpc_generated/flow';
import { Dispatch, SetStateAction } from 'react';

export type MirrorConfig = FlowConnectionConfigs;
export type MirrorSetter = Dispatch<SetStateAction<MirrorConfig>>;
export type TableMapRow = { source: string; destination: string };
