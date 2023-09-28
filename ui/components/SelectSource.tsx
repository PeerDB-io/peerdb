"use client"
import { Select, SelectItem } from '@/lib/Select';
import { DBType } from '@/grpc_generated/peers';
import { Dispatch, SetStateAction } from 'react';

interface SelectSourceProps{
    peerType: string;
    setPeerType: Dispatch<SetStateAction<string>>
}

export default function SelectSource({peerType,setPeerType}:SelectSourceProps){
    const dbTypes: string[] = Object.values(DBType)
        .filter((value): value is string => typeof value === 'string' 
        && value !== 'UNRECOGNIZED'
        && value !== 'MONGO'
    );
    
    return(
        <Select placeholder='Select a source' id='source' onValueChange={val => setPeerType(val)} children={
            dbTypes.map((dbType, id) => {
                return(
                    <SelectItem key={id} value={dbType}>
                        {dbType}
                    </SelectItem>
                )
            })
        }/>
    )
}