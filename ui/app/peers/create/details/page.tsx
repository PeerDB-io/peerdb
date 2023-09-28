"use client"
import { Label } from '@/lib/Label';
import { LayoutMain } from '@/lib/Layout';
import { Button } from '@/lib/Button';
import { ButtonGroup } from '@/lib/ButtonGroup';
import { Panel } from '@/lib/Panel';
import { RowWithTextField } from '@/lib/Layout';
import { TextField } from '@/lib/TextField';
import { useSearchParams } from 'next/navigation';
import Link from 'next/link';
import PgConfig from './configForm';
import { useState } from 'react';
import { pgConfigData } from './helpers/pg';
import { PeerConfig } from './types';
import { handleValidate } from './handlers';

export default function CreateConfig() {
    const searchParams = useSearchParams();
    const dbType = searchParams.get('dbtype')||"";
    const [name,setName] = useState<string>("");
    const [config, setConfig] = useState<PeerConfig>({
        host:"",
        port:5432,
        user:"",
        password:"",
        database:"",
        transactionSnapshot:""
    }) 
    const [formErr, setFormErr] = useState<string>("")
    const configComponentMap = (dbType:string) => {
        switch(dbType){
            case "POSTGRES":
                return <PgConfig configData={pgConfigData}  setter={setConfig}/>
            default:
                return <></>
        }
    }

    return(
        <LayoutMain alignSelf='center' justifySelf='center' width='xxLarge'>
        <Panel>
        <Label variant='title3'>New peer</Label>
        <Label colorName='lowContrast'>
            Set up a new peer.
        </Label>
        </Panel>
        <Panel>
        <Label colorName='lowContrast' variant='subheadline'>
        Details
        </Label>
        <RowWithTextField
            label={
            <Label as='label'>
                Name
            </Label>
            }
            action={<TextField variant="simple" onChange={(e)=>setName(e.currentTarget.textContent||"")}/>}
        />
        {
            dbType && configComponentMap(dbType)
        }
        </Panel>
        <Panel>
        <ButtonGroup>
            <Button as={Link} href='/peers/create'>Back</Button>
            <Button style={{backgroundColor:"gold"}} onClick={()=>handleValidate(
                dbType,
                config, 
                setFormErr,
                name
            )}>Validate</Button>
            <Button variant='normalSolid'>Create</Button>
        </ButtonGroup>
        <Panel>
            <Label colorName='lowContrast' variant='subheadline'>
            {formErr}
            </Label>
        </Panel>
        </Panel>
    </LayoutMain>
    )
}
