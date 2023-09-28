import { Dispatch, SetStateAction } from 'react';
import { PeerConfig, PeerSetter } from '../types';

const pgFields = [{
    label: "Host"
},{
    label: "Port",
    type: "number" // type for textfield
},{
    label: "User"
},{
    label: "Password",
    type: "password"
},{
    label: "Database",
}]

const pgHandler = (fieldType: string, value:string, 
    setter: PeerSetter) => {
    switch(fieldType){
        case "Host":
            setter(curr => ({...curr, host:value}))
            break;
        case "Port":
            setter(curr => ({...curr, port:parseInt(value, 10)}))
            break;
        case "User":
            setter(curr => ({...curr, user:value}))
            break;
        case "Password":
            setter(curr => ({...curr, password:value}))
            break;
        case "Database":
            setter(curr => ({...curr, database:value}))
            break;
        default:
            break;
    }
}

export const pgConfigData = {
    fields: pgFields,
    handler: pgHandler
}
