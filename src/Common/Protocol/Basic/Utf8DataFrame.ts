import {BinaryMessageType, UTF8DataMessage, UUID} from "../defs.js";
import {BufferUtil} from "../BufferUtil.js";
import {CryoBuffer} from "../../Wrappers/CryoBuffer.js";

export class Utf8DataFrame {
    public static Deserialize(value: CryoBuffer): UTF8DataMessage {
        const sid = BufferUtil.sidFromBuffer(value);
        const type = value.readUint8(16);
        const ack = value.readUint32BE(17);
        const payload = value.subarray(21).toString("utf8");

        if (type !== BinaryMessageType.UTF8DATA)
            throw new Error("Attempt to deserialize a non-data utf8 message!");

        return {
            sid,
            ack,
            type,
            payload
        }
    }

    public static Serialize(sid: UUID, ack: number, payload: string | null): CryoBuffer {
        const msg_buf = CryoBuffer.alloc(16 + 4 + 1 + (payload ? CryoBuffer.from(payload).byteLength : 4));
        const sid_buf = BufferUtil.sidToBuffer(sid);

        sid_buf.copy(msg_buf, 0);
        msg_buf.writeUint8(BinaryMessageType.UTF8DATA, 16);
        msg_buf.writeUint32BE(ack, 17);

        msg_buf.write(payload || "null", 21);

        return msg_buf;
    }
}