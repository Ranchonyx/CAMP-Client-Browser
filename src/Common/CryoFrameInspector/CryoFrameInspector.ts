import {BinaryMessageType} from "../Protocol/defs.js";
import {BufferUtil} from "../Protocol/BufferUtil.js";
import {CryoBuffer} from "../CryoBuffer/CryoBuffer.js";

const typeToStringMap: Record<BinaryMessageType, string> = {
    0: "ack",
    1: "error",
    2: "ping/pong",
    3: "utf8data",
    4: "binarydata",
    5: "transaction_start",
    6: "transaction_chunk",
    7: "transaction_finish",
}

export class CryoFrameInspector {
    public static Inspect(message: CryoBuffer): string {
        const sid = BufferUtil.GetSid(message);
        const type = BufferUtil.GetType(message);
        const type_str = typeToStringMap[type] || "unknown";
        const ack = BufferUtil.GetAck(message);

        if (type >= BinaryMessageType.TX_START) {
            switch (type) {
                case BinaryMessageType.TX_START:
                case BinaryMessageType.TX_FINISH:
                    return `[${sid},${ack},${BufferUtil.Transaction.GetTxId(message)},${type_str}]`;
                case BinaryMessageType.TX_CHUNK:
                    return `[${sid},${BufferUtil.Transaction.GetChunkTxId(message)},${type_str},[chunk_payload,len=${BufferUtil.Transaction.GetChunkPayload(message).length}]]`;
            }
            throw new Error("Unknown type " + type);
        } else {
            if (type === BinaryMessageType.BINARYDATA) {
                const payload = BufferUtil.GetPayload(message);
                return `[${sid},${ack},${type_str},[binary_payload,len=${payload.length}]]`;
            } else {
                const payload = BufferUtil.GetPayload(message);
                return `[${sid},${ack},${type_str},[utf8_payload,len=${payload.length}]]`;
            }
        }
    }
}