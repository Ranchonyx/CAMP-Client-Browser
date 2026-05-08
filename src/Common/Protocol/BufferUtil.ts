import {BinaryMessageType, UUID} from "./defs.js";
import {CryoBuffer} from "../Wrappers/CryoBuffer.js";

export class BufferUtil {
    public static sidFromBuffer(buffer: CryoBuffer): UUID {
        const uuidv4_p1 = buffer.subarray(0, 4).toString("hex");
        const uuidv4_p2 = buffer.subarray(4, 6).toString("hex");
        const uuidv4_p3 = buffer.subarray(6, 8).toString("hex");
        const uuidv4_p4 = buffer.subarray(8, 10).toString("hex");
        const uuidv4_p5 = buffer.subarray(10, 16).toString("hex");

        return [uuidv4_p1, uuidv4_p2, uuidv4_p3, uuidv4_p4, uuidv4_p5].join("-") as UUID;
    }

    public static sidToBuffer(sid: UUID): CryoBuffer {
        return CryoBuffer.from(sid.replaceAll("-", ""), 'hex');
    }

    public static GetType(message: CryoBuffer): BinaryMessageType {
        const type = message.readUint8(16);
        if (type > BinaryMessageType.TX_FINISH)
            throw new Error(`Unable to decode type from message ${message}. MAX_TYPE = 7, got ${type} !`);

        return type;
    }

    public static GetAck(message: CryoBuffer): number {
        return message.readUint32BE(17);
    }

    public static GetSid(message: CryoBuffer): UUID {
        return BufferUtil.sidFromBuffer(message);
    }

    public static GetPayload(message: CryoBuffer, encoding: "utf8" | "hex" = "utf8"): string {
        return message.subarray(21).toString(encoding);
    }

    public static Transaction = class {
        public static GetChunkTxId(message: CryoBuffer) {
            return message.readUint32BE(17);
        }

        public static GetChunkPayload(message: CryoBuffer, encoding: "utf8" | "hex" = "utf8"): string {
            return message.subarray(21).toString(encoding);
        }

        public static GetTxId(message: CryoBuffer) {
            return message.readUint32BE(21);
        }
    }
}