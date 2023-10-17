import { FlowConnectionConfigs } from '@/grpc_generated/flow';

type CDCDetailsProps = {
  config: FlowConnectionConfigs | undefined;
};

export default function CDCDetails({ config }: CDCDetailsProps) {
  if (!config) {
    return <div className='text-red-500'>No configuration provided</div>;
  }

  return (
    <div className='p-4 rounded-md'>
      <h2 className='text-xl font-semibold mb-4'>CDC Details</h2>
      <div className='overflow-x-auto'>
        <table className='min-w-full divide-y divide-gray-300'>
          <tr>
            <td className='px-4 py-2 font-medium'>Source</td>
            <td className='px-4 py-2'>{config.source?.name || '-'}</td>
          </tr>
          <tr>
            <td className='px-4 py-2 font-medium'>Destination</td>
            <td className='px-4 py-2'>{config.destination?.name || '-'}</td>
          </tr>
          <tr>
            <td className='px-4 py-2 font-medium'>Flow Job Name</td>
            <td className='px-4 py-2'>{config.flowJobName}</td>
          </tr>
        </table>
      </div>
    </div>
  );
}
