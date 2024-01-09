import prisma from '@/app/utils/prisma';
import { QRepConfig } from '@/grpc_generated/flow';
import { Badge } from '@/lib/Badge';
import { Icon } from '@/lib/Icon';
import { Label } from '@/lib/Label';
import { ProgressCircle } from '@/lib/ProgressCircle';

type QRepConfigViewerProps = {
  mirrorId: string;
};

export default async function QRepConfigViewer({
  mirrorId,
}: QRepConfigViewerProps) {
  const configBuffer = await prisma.qrep_runs.findFirst({
    select: {
      config_proto: true,
    },
    where: {
      flow_name: mirrorId,
      config_proto: {
        not: null,
      },
    },
  });

  if (!configBuffer?.config_proto) {
    return (
      <div className='m-4' style={{ display: 'flex', alignItems: 'center' }}>
        <ProgressCircle variant='determinate_progress_circle' />
        <Label>Waiting for mirror to start...</Label>
      </div>
    );
  }

  let qrepConfig = QRepConfig.decode(configBuffer.config_proto);

  return (
    <div className='my-4'>
      <Badge type='longText'>
        <Icon name={qrepConfig.initialCopyOnly ? 'double_arrow' : 'sync'} />
        <div className='font-bold'>
          {qrepConfig.initialCopyOnly ? 'Initial Load' : 'Continuous Sync'}
        </div>
      </Badge>
    </div>
  );
}
