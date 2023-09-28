"use client"
import { Label } from '@/lib/Label';
import { RowWithTextField } from '@/lib/Layout';
import { TextField } from '@/lib/TextField';
import { Dispatch, SetStateAction } from 'react';
import { PeerConfig, PeerSetter } from './types';

interface Field {
    label: string,
    type?: string
}

interface ConfigData{
    fields: Field[]
    handler: (fieldType: string, value:string, 
        setter:PeerSetter) => void
}

interface ConfigProps{
    configData: ConfigData
    setter: PeerSetter
}

export default function PgConfig(props: ConfigProps) {
    return(
    <>
    {props.configData.fields.map((field) => {
        return(
            <RowWithTextField
                label={
                <Label as='label'>
                    {field.label}
                </Label>
                }
                action={<TextField variant="simple" type={field.type} onChange={
                    e => props.configData.handler(field.label, e.target.value, props.setter)
                }/>}
            />
        )
        })
    }
    </>
    )
}