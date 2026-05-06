import {CryoBuffer} from "../../Common/CryoBuffer/CryoBuffer.js";

export interface ICryoClientWebsocketSessionEvents {
    "message-utf8": string;
    "message-binary": CryoBuffer;
    "closed": [number, string];
    "connected": undefined;
    "disconnected": undefined;
    "reconnected": undefined;

    "tx-start": [txId: number, txName: string];
    "tx-chunk": [txId: number, data: Buffer];
    "tx-finish": number;
}

export type PendingBinaryMessage = {
    timestamp: number;
    message: CryoBuffer;
    payload?: string | CryoBuffer;
}
