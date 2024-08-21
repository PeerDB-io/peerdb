import { PeerStatResponse } from '@/grpc_generated/route';
import { GetFlowHttpAddressFromEnv } from '@/rpc/http';
import { NextRequest, NextResponse } from 'next/server';

export async function GET(
  request: NextRequest,
  context: { params: { name: string } }
) {
  try {
    const flowServiceAddr = GetFlowHttpAddressFromEnv();
    const peerStats: PeerStatResponse = await fetch(
      `${flowServiceAddr}/v1/peers/stats/${context.params.name}`,
      { cache: 'no-store' }
    )
      .then((res) => res.json())
      .catch((e) => {
        console.error('Error fetching slots:', e);
        return [];
      });
    return NextResponse.json(peerStats.statData);
  } catch (e) {
    console.error('Error fetching slots:', e);
    return NextResponse.error();
  }
}
