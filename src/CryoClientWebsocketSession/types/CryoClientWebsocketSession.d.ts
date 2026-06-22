import {CAMPBuffer} from "camp-protocol";

export interface ICryoClientWebsocketSessionEvents {
    "message-utf8": string;
    "message-binary": CAMPBuffer;
    "message-error": string;
    "closed": [number, string];
    "connected": undefined;
    "disconnected": undefined;
    "reconnected": undefined;

    "tx-start": [txId: number, txName: string, txLength: bigint | null];
    "tx-chunk": [txId: number, data: CAMPBuffer];
    "tx-finish": number;
    "tx-fetch": [txId: number, start: bigint, end: bigint];
}

export type PendingBinaryMessage = {
    timestamp: number;
    message: CryoBuffer;
    payload?: string | CAMPBuffer;
}
