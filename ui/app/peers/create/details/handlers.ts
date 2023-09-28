import { DBType } from "@/grpc_generated/peers";
import { ValidatePeerRequest } from "@/grpc_generated/route";
import { GetFlowServiceClient } from "@/rpc/rpc";
import { PeerConfig } from "./types";
import { pgSchema } from "./schema";
import { Dispatch, SetStateAction } from "react";

const typeMap = (type:string)=> {
   switch(type){
    case "POSTGRES": return DBType.POSTGRES
    case "SNOWFLAKE": return DBType.SNOWFLAKE
    default: return DBType.UNRECOGNIZED
   }
}
export const handleValidate = async (
    type:string,
    config:PeerConfig, 
    setErr:Dispatch<SetStateAction<string>>,
    name?:string
    ) => {
    if (!name){
        setErr("Peer name is required");
        return;
    }
    const validity = pgSchema.validate(config);
    if(validity.error){
        setErr(validity.error.message);
        return;
    }
    let flowServiceAddress = process.env.PEERDB_FLOW_SERVER_ADDRESS!;
    let flowServiceClient = GetFlowServiceClient(flowServiceAddress);
    let req: ValidatePeerRequest = {
        peer:{
            "name": name,
            "type": typeMap(type),
            "postgresConfig": config
          }
    };
    let peers = await flowServiceClient.listPeers(req);
    return peers.peers;
}


const handleCreate = () => {
        
}