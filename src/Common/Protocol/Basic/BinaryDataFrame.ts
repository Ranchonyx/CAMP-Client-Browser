import {BinaryDataMessage, BinaryMessageType, UUID} from "../defs.js";
import {BufferUtil} from "../BufferUtil.js";
import {CryoBuffer} from "../../CryoBuffer/CryoBuffer.js";

export class BinaryDataFrame {
    public static Deserialize(value: CryoBuffer): BinaryDataMessage {
        const sid = BufferUtil.sidFromBuffer(value);
        const type = value.readUint8(16);
        const ack = value.readUint32BE(17);
        const payload = value.subarray(21);

        if (type !== BinaryMessageType.BINARYDATA)
            throw new Error("Attempt to deserialize a non-data binary message!");

        return {
            sid,
            ack,
            type,
            payload
        }
    }

    public static Serialize(sid: UUID, ack: number, payload: CryoBuffer | null): CryoBuffer {
        const payload_length = payload ? payload.byteLength : 4;
        const msg_buf = CryoBuffer.alloc(16 + 4 + 1 + payload_length);
        const sid_buf = BufferUtil.sidToBuffer(sid);

        sid_buf.copy(msg_buf, 0);
        msg_buf.writeUint8(BinaryMessageType.BINARYDATA, 16);
        msg_buf.writeUint32BE(ack, 17);

        msg_buf.set(payload || CryoBuffer.from("null", "utf8"), 21);

        return msg_buf;
    }
}