import {
    ACKFrame,
    BufferUtil,
    CAMP_FEATURE_MASK_TRANSACTION,
    CAMP_FLOW_BEHAVIOUR, CAMPBuffer,
    CAMPFrameType,
    CAMPHasFeatureFlag,
    TXCancelFrame,
    TXChunkFrame,
    TXFetchFrame,
    TXFinishFrame,
    TXStartFrame
} from "camp-protocol";
import {CAMPEventEmitter} from "../../Common/CryoEventEmitter/CAMPEventEmitter.js";
import {CAMPReadable} from "../../Common/Wrappers/CAMPReadable.js";

type Stream = {
    readable: ReadableStream<Uint8Array>;
    controller: ReadableStreamDefaultController<Uint8Array>;
    name: string;
    claimed: boolean;
};

interface CAMPTransactionManagerEvents {
    "tx-start": [txId: number, txName: string, txLength: bigint | null];
    "tx-chunk": [txId: number, data: CAMPBuffer];
    "tx-finish": [txId: number];
    "tx-fetch": [txId: number, start: bigint, end: bigint];
}

export class CAMPTransactionManager extends CAMPEventEmitter<CAMPTransactionManagerEvents> {
    private STREAM_KEEP_TIMEOUT = 30_000;

    private readonly incomingStreams = new Map<number, Stream>();
    private readonly outgoingStreams = new Map<number, AbortController>();

    public constructor(
        private sid: bigint,
        private send: (frame: CAMPBuffer) => Promise<void>,
        private next_ack: () => number,
        private next_txid: () => number,
        private destroy: (code?: number, message?: string) => void,
        private get_features: () => bigint
    ) {
        super();
    }

    /**
     * Stream a readable to the client
     * @param source The {@link ReadableStream} object to be streamed
     * @param options The options for streaming the readable
     * */
    public async Stream(source: ReadableStream<Uint8Array>, options: {
        streamName: string,
        behaviour: "pull" | "push"
    } = {streamName: "anonymous", behaviour: "push"}) {
        const bhv = options.behaviour === "pull" ? CAMP_FLOW_BEHAVIOUR.TX_PULL : CAMP_FLOW_BEHAVIOUR.TX_PUSH;
        if (bhv === CAMP_FLOW_BEHAVIOUR.TX_PUSH)
            return this.StreamPush(source, options.streamName);
        return this.StreamPull(source, options.streamName);
    }

    /**
     * Wait for an incoming stream
     * @param streamName The name of the stream to wait for - leave empty to wait for an unnamed stream
     * @param timeout The amount of milliseconds to wait until the operation should be cancelled if no matching stream was received
     * */
    public async WaitForStream(streamName: string = "anonymous", timeout: number = 1000): Promise<CAMPReadable> {
        const timeoutSig = AbortSignal.timeout(timeout);

        return new Promise<CAMPReadable>((resolve, reject) => {
            const onAbort = () => {
                this.off("tx-start", onTxStartListener);
                timeoutSig.removeEventListener("abort", onAbort);
                reject(new Error(`Timeout elapsed!`));
            }

            const cleanup = () => {
                this.off("tx-start", onTxStartListener);
                timeoutSig.removeEventListener("abort", onAbort);
            };

            const tryResolveStream = (txId: number, txName: string, txLength: bigint | null): boolean => {
                if (txName !== streamName)
                    return false;

                if (!this.incomingStreams.has(txId)) {
                    cleanup();
                    reject(new Error(`No stream id ${txId} present!`));
                    return true;
                }


                const stream = this.incomingStreams.get(txId)!;
                if (stream.claimed)
                    return false;

                stream.claimed = true;
                cleanup();

                resolve(new CAMPReadable(stream.readable, txId, txLength, (txIdToDelete) => this.incomingStreams.delete(txIdToDelete)));
                return true;
            };

            const onTxStartListener = (data: [txId: number, txName: string, txLength: bigint | null]) => {
                const [txId, txName, txLength] = data;
                tryResolveStream(txId, txName, txLength);
            };

            for (const [txId, stream] of this.incomingStreams.entries()) {
                if (stream.claimed)
                    continue;

                if (tryResolveStream(txId, stream.name, null))
                    return;
            }

            this.on("tx-start", onTxStartListener);
            timeoutSig.addEventListener("abort", onAbort);
        });
    }

    /**
     * Request a range of bytes from the stream - used when flow control = TX_PULL
     * @param stream The readable object returned by {@link WaitForStream}
     * @param start The starting index of bytes to be requested
     * @param end The ending index of bytes to be requested
     * */
    public async StreamRequestRange(stream: CAMPReadable, start: bigint, end: bigint): Promise<void> {
        const fetch_ack_id = this.next_ack();
        const fetch_frame = TXFetchFrame.Serialize(this.sid, fetch_ack_id, stream.txId, start, end);
        await this.send(fetch_frame);
    }

    private async StreamPush(source: ReadableStream<Uint8Array>, streamName: string): Promise<void> {
        const new_txid = this.next_txid();
        const controller = new AbortController();
        const signal = controller.signal;
        this.outgoingStreams.set(new_txid, controller);

        try {
            //Send tx_start
            const start_ack_id = this.next_ack();
            const start_frame = TXStartFrame.Serialize(this.sid, start_ack_id, new_txid, streamName, -1n, CAMP_FLOW_BEHAVIOUR.TX_PUSH);
            await this.send(start_frame);

            //Send tx_chunk
            let offset = 0n;
            const reader = source.getReader();

            try {
                let seq = 0;
                while (true) {

                    const {value, done} = await reader.read();
                    if (done)
                        break;

                    offset += BigInt(value.byteLength);
                    const chunk_frame = TXChunkFrame.Serialize(this.sid, new_txid, offset, new CAMPBuffer(value));
                    signal.throwIfAborted();
                    await this.send(chunk_frame);
                }
            } finally {
                reader.releaseLock();
            }

            //send tx_finish
            const finish_ack_id = this.next_ack();
            const finish_frame = TXFinishFrame.Serialize(this.sid, finish_ack_id, new_txid);
            await this.send(finish_frame);

        } catch (reason) {
            //this.log(`Transaction ${new_txid} was aborted by client.`);
        } finally {
            this.outgoingStreams.delete(new_txid);
        }
    }

    private PullTransaction = class {
        public byteLength = 0n;

        private chunks: Uint8Array[] = [];

        constructor(
            private source: ReadableStream<Uint8Array>,
            public txId: number,
        ) {
        }

        /**
         * Buffers the stream once on disk in a temporary directory
         * */
        public async setup() {
            const reader = this.source.getReader();

            while (true) {
                const {done, value} = await reader.read();
                if (done) break;

                this.chunks.push(value);
                this.byteLength += BigInt(value.byteLength);
            }

        }

        public getRangedStream(start: bigint, end: bigint) {
            const chunks = this.chunks;
            let offset = 0n;
            let index = 0;

            return new ReadableStream<Uint8Array>({
                pull(controller) {
                    while (index < chunks.length) {
                        const chunk = chunks[index];
                        const chunkStart = offset;
                        const chunkEnd = offset + BigInt(chunk.byteLength) - 1n;

                        offset += BigInt(chunk.byteLength);
                        index++;

                        if (chunkEnd < start)
                            continue;

                        if (chunkStart > end)
                            return;

                        const lStart = start > chunkStart ? Number(start - chunkStart) : 0;
                        const lEnd = end < chunkEnd ? Number(end - chunkStart) : 0;

                        controller.enqueue(chunk.subarray(lStart, lEnd));
                    }

                    controller.close();
                }
            });
        }

        public async dispose() {
            this.chunks = [];
        }
    }

    private StreamPull(source: ReadableStream<Uint8Array>, streamName: string): Promise<void> {
        return new Promise<void>(async (resolve) => {
            const start_ack_id = this.next_ack();
            const new_txid = this.next_txid();

            const controller = new AbortController();
            const signal = controller.signal;
            this.outgoingStreams.set(new_txid, controller);

            const tran = new this.PullTransaction(source, new_txid);
            await tran.setup();

            let fetchHandler: ((params: [txId: number, start: bigint, end: bigint]) => Promise<void>) | null = null;

            const cleanup = async () => {
                if (fetchHandler)
                    this.off("tx-fetch", fetchHandler);

                this.outgoingStreams.delete(new_txid);

                await tran.dispose();
            }

            try {
                const start_frame = TXStartFrame.Serialize(this.sid, start_ack_id, new_txid, streamName, tran.byteLength, CAMP_FLOW_BEHAVIOUR.TX_PULL);
                await this.send(start_frame);

                await new Promise<void>((resolve) => {
                    fetchHandler = async (params: [txId: number, start: bigint, end: bigint]) => {
                        const [txId, start, end] = params;
                        if (txId !== new_txid)
                            return;

                        try {
                            signal.throwIfAborted();
                            await this.sendRange(tran, txId, start, end, signal);

                            if (end + 1n >= tran.byteLength) {
                                const finish_ack_id = this.next_ack();
                                const finish_frame = TXFinishFrame.Serialize(this.sid, finish_ack_id, new_txid);

                                await this.send(finish_frame);
                            }
                        } catch {
                            resolve();
                        }

                    }

                    this.on("tx-fetch", fetchHandler);
                });
            } finally {
                await cleanup();
            }
        });
    }

    private async sendRange(tran: InstanceType<typeof this.PullTransaction>, txId: number, start: bigint, end: bigint, signal: AbortSignal) {
        const rangeStream = tran.getRangedStream(start, end);
        const reader = rangeStream.getReader();

        let offset = start;
        try {
            while (true) {
                signal.throwIfAborted();

                const {value, done} = await reader.read();

                if (done)
                    break;

                const chunkFrame = TXChunkFrame.Serialize(
                    this.sid,
                    txId,
                    offset,
                    new CAMPBuffer(value)
                );

                await this.send(chunkFrame);

                offset += BigInt(value.byteLength);
            }
        } finally {
            reader.releaseLock();
        }
    }

    public async handle(frame: CAMPBuffer) {
        const type = BufferUtil.GetType(frame);

        switch (type) {
            case CAMPFrameType.TX_START:
                await this.HandleTxStart(frame);
                return;
            case CAMPFrameType.TX_CHUNK:
                await this.HandleTxChunk(frame);
                return;
            case CAMPFrameType.TX_FINISH:
                await this.HandleTxFinish(frame);
                return;
            case CAMPFrameType.TX_FETCH:
                await this.HandleTxFetch(frame);
                return;
            case CAMPFrameType.TX_CANCEL:
                await this.HandleTxCancel(frame);
        }
    }

    private async HandleTxStart(frame: CAMPBuffer) {
        if (this.abortIfMismatched())
            return;

        const decodedStartFrame = TXStartFrame
            .Deserialize(frame);

        const ack_id = decodedStartFrame.ack;
        const encodedACKMessage = ACKFrame
            .Serialize(this.sid, ack_id);

        await this.send(encodedACKMessage);

        let controller!: ReadableStreamDefaultController<Uint8Array>;
        const readable = new ReadableStream<Uint8Array>({
            start(c) {
                controller = c;
            },
            cancel: () => {
                this.incomingStreams.delete(decodedStartFrame.txId);
            }
        });

        this.incomingStreams.set(decodedStartFrame.txId, {
            controller,
            readable,
            name: decodedStartFrame.txName,
            claimed: false
        });

        await this.acknowledge(decodedStartFrame.txId);
        this.emit("tx-start", [decodedStartFrame.txId, decodedStartFrame.txName, decodedStartFrame.byteLength]);

    }

    private async HandleTxCancel(frame: CAMPBuffer) {
        if (this.abortIfMismatched())
            return;

        const decodedCancelFrame = TXCancelFrame
            .Deserialize(frame);

        const {txId} = decodedCancelFrame;

        if (!this.outgoingStreams.has(txId))
            return;

        await this.acknowledge(decodedCancelFrame.ack);
        this.outgoingStreams.get(txId)?.abort("Cancelled by client.");
    }

    private async HandleTxFinish(frame: CAMPBuffer) {
        if (this.abortIfMismatched())
            return;

        const decodedFinishFrame = TXFinishFrame
            .Deserialize(frame);

        await this.acknowledge(decodedFinishFrame.ack);
        const stream = this.incomingStreams.get(decodedFinishFrame.txId);
        if (!stream)
            return;

        stream.controller.close();
        if (!stream.claimed)
            setTimeout(() => {
                const currentStream = this.incomingStreams.get(decodedFinishFrame.txId);
                if (currentStream && !currentStream.claimed)
                    this.incomingStreams.delete(decodedFinishFrame.txId);

            }, this.STREAM_KEEP_TIMEOUT);
        this.emit("tx-finish", [decodedFinishFrame.txId]);
    }

    private async HandleTxFetch(frame: CAMPBuffer) {
        if (this.abortIfMismatched())
            return;

        const decodedFetchFrame = TXFetchFrame
            .Deserialize(frame);

        await this.acknowledge(decodedFetchFrame.ack);

        this.emit("tx-fetch", [decodedFetchFrame.txId, decodedFetchFrame.start, decodedFetchFrame.end]);
    }

    private async HandleTxChunk(frame: CAMPBuffer) {
        if (this.abortIfMismatched())
            return;

        const decodedChunkFrame = TXChunkFrame
            .Deserialize(frame);

        //Handle stream
        if (!this.incomingStreams.has(decodedChunkFrame.txId))
            return;

        this.incomingStreams.get(decodedChunkFrame.txId)!.controller.enqueue(decodedChunkFrame.payload.buffer.slice());

        this.emit("tx-chunk", [decodedChunkFrame.txId, decodedChunkFrame.payload]);
    }

    private async acknowledge(ack_id: number) {
        const encodedACKMessage = ACKFrame
            .Serialize(this.sid, ack_id);

        await this.send(encodedACKMessage);
    }

    private abortIfMismatched() {
        if (!CAMPHasFeatureFlag(this.get_features(), CAMP_FEATURE_MASK_TRANSACTION)) {
            this.destroy(4002, "PROTOCOL FEATURE MISMATCH - The connected client does not support features in the namespace 'CAMP.Transaction' !");
            return true;
        }

        return false;
    }
}