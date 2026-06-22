import {CAMPFrameType, BufferUtil, CAMPBuffer} from "camp-protocol";

const typeToStringMap: Record<CAMPFrameType, string> = {
    255: "endpoint_info",
    254: "bye",
    253: "ack",
    252: "error",
    251: "ping/pong",
    250: "utf8data",
    249: "binarydata",
    0: "transaction_start",
    1: "transaction_chunk",
    2: "transaction_finish",
    3: "transaction_chunk_request",
    4: "transaction_cancel",
}

export class CAMPFrameInspector {
    public static Inspect(message: CAMPBuffer): string {
        const sid = BufferUtil.GetSid(message);
        const type = BufferUtil.GetType(message);
        const type_str = typeToStringMap[type] || "unknown";
        const ack = BufferUtil.GetAck(message);

        //For CAMP.Transaction
        if (type >= CAMPFrameType.TX_START && type <= CAMPFrameType.TX_CANCEL) {
            switch (type) {
                case CAMPFrameType.TX_START:
                    return `[type=${type_str}, sid=${sid},ack=${ack},txid=${BufferUtil.Transaction.GetTxId(message)},name=${BufferUtil.Transaction.GetTxName(message)}]`;
                case CAMPFrameType.TX_FINISH:
                    return `[type=${type_str}, sid=${sid},ack=${ack},txid=${BufferUtil.Transaction.GetTxId(message)}]`;
                case CAMPFrameType.TX_CHUNK:
                    return `[type=${type_str}, sid=${sid},txid=${BufferUtil.Transaction.GetChunkTxId(message)},payload[0..15]=${BufferUtil.Transaction.GetChunkPayload(message, "hex").substring(0, 0xf)}]`;
                case CAMPFrameType.TX_CANCEL:
                    return `[type=${type_str}, sid=${sid},ack=${ack},txid=${BufferUtil.Transaction.GetTxId(message)}]`;
            }
            throw new Error("Unknown type " + type);
        } else {
            const payload = BufferUtil.GetPayload(message, "hex").substring(0, 0xf);
            return `[type=${type_str}, sid=${sid},ack=${ack},payload[0..15]=${payload}]`;
        }
    }
}