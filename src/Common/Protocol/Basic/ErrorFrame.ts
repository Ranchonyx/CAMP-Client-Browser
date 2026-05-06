import {BufferUtil} from "../BufferUtil.js";
import {BinaryMessageType, ErrorMessage, UUID} from "../defs.js";
import {CryoBuffer} from "../../CryoBuffer/CryoBuffer.js";

export class ErrorFrame {
    public static Deserialize(value: CryoBuffer): ErrorMessage {
        const sid = BufferUtil.sidFromBuffer(value);
        const type = value.readUint8(16);
        const ack = value.readUint32BE(17);
        const payload = value.subarray(21).toString("utf8") as ErrorMessage["payload"];

        if (type !== BinaryMessageType.ERROR)
            throw new Error("Attempt to deserialize a non-error message!");

        return {
            sid,
            ack,
            type,
            payload
        }
    }

    public static Serialize(sid: UUID, ack: number, payload: ErrorMessage["payload"] | null): CryoBuffer {
        const msg_buf = CryoBuffer.alloc(16 + 4 + 1 + (payload?.length || 13));
        const sid_buf = BufferUtil.sidToBuffer(sid);

        sid_buf.copy(msg_buf, 0);
        msg_buf.writeUint8(BinaryMessageType.ERROR, 16);
        msg_buf.writeUint32BE(ack, 17);

        msg_buf.write(payload || "unknown_error", 21);

        return msg_buf;
    }
}