import { alertConfigType } from '@/app/alert-config/validation';
import prisma from '@/app/utils/prisma';
import { dynamic_settings } from '@prisma/client';

export async function GET() {
    try {
        const configs: dynamic_settings[] = await prisma.dynamic_settings.findMany();
        const serializedConfigs = configs.map((config) => ({
            ...config,
            id: config.id,
        }));
        return new Response(JSON.stringify(serializedConfigs));
    } catch (error) {
        console.error('Error fetching dynamic settings:', error);
        return new Response(JSON.stringify({ error: 'Failed to fetch dynamic settings' }), { status: 500 });
    }
}

export async function POST(request: Request) {
    try {
        const configReq: dynamic_settings = await request.json();
        const updateRes = await prisma.dynamic_settings.update({
            where: {
                id: configReq.id,
            },
            data: {
                config_value: configReq.config_value,
            },
        });

        let updateStatus: 'success' | 'error' = 'error';
        if (updateRes.id) {
            updateStatus = 'success';
        }

        return new Response(updateStatus);
    } catch (error) {
        console.error('Error updating dynamic setting:', error);
        return new Response(JSON.stringify({ error: 'Failed to update dynamic setting' }), { status: 500 });
    }
}

