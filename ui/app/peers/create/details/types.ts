import { PostgresConfig } from "@/grpc_generated/peers";
import { Dispatch, SetStateAction } from 'react';

export type PeerConfig = PostgresConfig
export type PeerSetter = Dispatch<SetStateAction<PeerConfig>>