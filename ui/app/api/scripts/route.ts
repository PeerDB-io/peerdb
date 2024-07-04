import { ScriptsType } from '@/app/dto/ScriptsDTO';
import prisma from '@/app/utils/prisma';
import { NextRequest, NextResponse } from 'next/server';

export async function GET(_: NextRequest) {
  const existingScripts = await prisma.scripts.findMany({
    orderBy: { name: 'asc' },
  });
  const scripts: ScriptsType[] = existingScripts.map((script) => ({
    ...script,
    source: script.source.toString(),
  }));

  return NextResponse.json(scripts);
}

export async function POST(request: Request) {
  const scriptReq: ScriptsType = await request.json();
  let createStatus: 'success' | string = 'error';
  try {
    const createRes = await prisma.scripts.create({
      data: {
        lang: scriptReq.lang,
        name: scriptReq.name,
        source: Buffer.from(scriptReq.source, 'utf-8'),
      },
    });
    if (createRes.id) createStatus = 'success';
  } catch (err) {
    createStatus = 'error';
  }

  return new Response(createStatus);
}

export async function DELETE(request: Request) {
  const scriptDeleteId = await request.json();
  let deleteStatus: 'success' | 'error' = 'error';
  try {
    const deleteRes = await prisma.scripts.delete({
      where: {
        id: scriptDeleteId,
      },
    });
    if (deleteRes.id) {
      deleteStatus = 'success';
    }
  } catch (err) {
    deleteStatus = 'error';
  }

  return new Response(deleteStatus);
}

export async function PUT(request: Request) {
  const scriptReq: ScriptsType = await request.json();
  let editStatus: 'success' | 'error' = 'error';
  try {
    const editRes = await prisma.scripts.update({
      data: {
        lang: scriptReq.lang,
        name: scriptReq.name,
        source: Buffer.from(scriptReq.source, 'utf-8'),
      },
      where: {
        id: scriptReq.id,
      },
    });
    if (editRes.id) {
      editStatus = 'success';
    }
  } catch (err) {
    editStatus = 'error';
  }

  return new Response(editStatus);
}
