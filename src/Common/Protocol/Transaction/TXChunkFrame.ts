import {BinaryMessageType, TXChunkMessage, UUID} from "../defs.js";
import {BufferUtil} from "../BufferUtil.js";
import {CryoBuffer} from "../../Wrappers/CryoBuffer.js";

export class TXChunkFrame {
    public static Deserialize(value: CryoBuffer): TXChunkMessage {
        const sid = BufferUtil.sidFromBuffer(value);
        const type = value.readUint8(16);
        const txId = value.readUint32BE(17);
        const payload = value.subarray(21);

        if (type !== BinaryMessageType.TX_CHUNK)
            throw new Error("Attempt to deserialize a non-tx_chunk message!");

        return {
            sid,
            type,
            txId,
            payload
        }
    }

    public static Serialize(sid: UUID, txId: number, payload: CryoBuffer): CryoBuffer {
        const msg_buf = CryoBuffer.alloc(16 + 1 + 4 + payload.byteLength);
        const sid_buf = BufferUtil.sidToBuffer(sid);

        sid_buf.copy(msg_buf, 0); //Write sid 0-16
        msg_buf.writeUint8(BinaryMessageType.TX_CHUNK, 16);
        msg_buf.writeUint32BE(txId, 17);
        msg_buf.set(payload, 21);

        return msg_buf;
    }
}