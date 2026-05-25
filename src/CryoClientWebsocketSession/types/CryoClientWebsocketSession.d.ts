import {CryoBuffer} from "../../Common/Wrappers/CryoBuffer.js";

export interface ICryoClientWebsocketSessionEvents {
    "message-utf8": string;
    "message-binary": CryoBuffer;
    "closed": [number, string];
    "connected": undefined;
    "disconnected": undefined;
    "reconnected": undefined;

    "tx-start": [txId: number, txName: string];
    "tx-chunk": [txId: number, data: CryoBuffer];
    "tx-finish": number;
    "tx-fetch": [txId: number, start: number, end: number];
}

export type PendingBinaryMessage = {
    timestamp: number;
    message: CryoBuffer;
    payload?: string | CryoBuffer;
}
