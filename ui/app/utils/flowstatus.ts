import { FlowStatus } from '@/grpc_generated/flow';

export function FormatStatus(mirrorStatus: FlowStatus) {
  const mirrorStatusLower = mirrorStatus
    .toString()
    .split('_')
    .at(-1)
    ?.toLocaleLowerCase()!;
  return (
    mirrorStatusLower.at(0)?.toLocaleUpperCase() + mirrorStatusLower.slice(1)
  );
}
