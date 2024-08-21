import { PeerSlotResponse } from '@/grpc_generated/route';
import { GetFlowHttpAddressFromEnv } from '@/rpc/http';
import { NextRequest, NextResponse } from 'next/server';

export async function GET(
  request: NextRequest,
  context: { params: { name: string } }
) {
  const flowServiceAddr = GetFlowHttpAddressFromEnv();
  try {
    const slotDataRes: PeerSlotResponse = await fetch(
      `${flowServiceAddr}/v1/peers/slots/${context.params.name}`,
      { cache: 'no-store' }
    ).then((res) => res.json());

    return new Response(JSON.stringify(slotDataRes));
  } catch (e) {
    console.error('Error fetching slots:', e);
    return NextResponse.error();
  }
}
