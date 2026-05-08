import {BinaryMessageType, TXFinishMessage, UUID} from "../defs.js";
import {BufferUtil} from "../BufferUtil.js";
import {CryoBuffer} from "../../Wrappers/CryoBuffer.js";

export class TXFinishFrame {
    public static Deserialize(value: CryoBuffer): TXFinishMessage {
        const sid = BufferUtil.sidFromBuffer(value);
        const type = value.readUint8(16);
        const ack = value.readUint32BE(17);
        const txId = value.readUint32BE(21);

        if (type !== BinaryMessageType.TX_FINISH)
            throw new Error("Attempt to deserialize a non-tx_finish message!");

        return {
            sid,
            ack,
            type,
            txId
        }
    }

    public static Serialize(sid: UUID, ack: number, txId: number): CryoBuffer {
        const msg_buf = CryoBuffer.alloc(16 + 4 + 1 + 4);
        const sid_buf = BufferUtil.sidToBuffer(sid);

        sid_buf.copy(msg_buf, 0);
        msg_buf.writeUint8(BinaryMessageType.TX_FINISH, 16);
        msg_buf.writeUint32BE(ack, 17);
        msg_buf.writeUint32BE(txId, 21);

        return msg_buf;
    }
}
