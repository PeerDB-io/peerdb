import { ConfigJSONView } from '@/app/alert-config/page';
import { getTruePeer } from '@/app/api/peers/getTruePeer';
import prisma from '@/app/utils/prisma';
import { Label } from '@/lib/Label';
import omitKeys from './omitKeys';

interface PeerDetailsProps {
  peerName: string;
}

const PeerDetails = async ({ peerName }: PeerDetailsProps) => {
  const peer = await prisma.peers.findFirst({
    where: {
      name: peerName,
    },
  });
  const peerConfig = getTruePeer(peer!);

  return (
    <div>
      <div
        style={{
          padding: '0.5rem',
          minWidth: '30%',
          marginTop: '1rem',
          marginBottom: '1rem',
        }}
      >
        <div>
          <div>
            <Label variant='subheadline'>Configuration:</Label>
          </div>
          <div
            style={{
              height: peerConfig.postgresConfig ? '14em' : '20em',
              whiteSpace: 'pre-wrap',
              marginTop: '1rem',
            }}
          >
            <ConfigJSONView
              config={JSON.stringify(
                peerConfig,
                (key, value) => {
                  if (omitKeys.includes(key)) {
                    return undefined;
                  }
                  return value;
                },
                2
              )}
            />
          </div>
        </div>
      </div>
    </div>
  );
};
export default PeerDetails;
