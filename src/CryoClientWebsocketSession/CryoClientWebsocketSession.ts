import {CryoFrameInspector} from "../Common/CryoFrameInspector/CryoFrameInspector.js";
import {CreateDebugLogger, DebugLoggerFunction} from "../Common/Util/CreateDebugLogger.js";
import {ICryoClientWebsocketSessionEvents, PendingBinaryMessage} from "./types/CryoClientWebsocketSession.js";
import {AckTracker} from "../Common/AckTracker/AckTracker.js";
import {CryoBuffer} from "../Common/Wrappers/CryoBuffer.js";
import {CryoEventEmitter} from "../Common/CryoEventEmitter/CryoEventEmitter.js";
import {
    ACKFrame,
    BinaryDataFrame,
    BinaryMessageType,
    BufferUtil,
    ErrorFrame,
    PingPongFrame, TXChunkFrame, TXFinishFrame, TXStartFrame,
    Utf8DataFrame
} from "cryo-protocol";
import {CryoStream} from "../Common/Wrappers/CryoStream.js";

type UUID = `${string}-${string}-${string}-${string}-${string}`;

enum CloseCode {
    CLOSE_GRACEFUL = 4000,
    CLOSE_CLIENT_ERROR = 4001,
    CLOSE_SERVER_ERROR = 4002,
    CLOSE_CALE_MISMATCH = 4010,
    CLOSE_CALE_HANDSHAKE = 4011
}

type Buffer = CryoBuffer;
type Stream = {
    readable: ReadableStream<Uint8Array>;
    controller: ReadableStreamDefaultController<Uint8Array>;
    name: string;
    claimed: boolean;
};

/*
* Cryo Websocket session layer. Handles Binary formatting and ACKs and whatnot
* */
export class CryoClientWebsocketSession extends CryoEventEmitter<ICryoClientWebsocketSessionEvents> {
    private messages_pending_server_ack = new Map<number, PendingBinaryMessage>();
    private server_ack_tracker = new AckTracker();
    private streams = new Map<number, Stream>();

    private STREAM_KEEP_TIMEOUT = 30_000;

    private bytes_tx = 0;
    private bytes_rx = 0;

    private current_ack = 0;
    private current_txid = 0;

    private static async ConstructSocket(host: string, timeout: number, bearer: string, sid: string): Promise<WebSocket> {
        const full_host_url = new URL(host);
        full_host_url.searchParams.set("authorization", `Bearer ${bearer}`);
        full_host_url.searchParams.set("x-cryo-sid", sid);
        const sck = new WebSocket(full_host_url);
        sck.binaryType = "arraybuffer";

        return new Promise<WebSocket>((resolve, reject) => {
            setTimeout(() => {
                if (sck.readyState !== WebSocket.OPEN)
                    reject(new Error(`Connection timeout of ${timeout} ms reached!`));
            }, timeout)
            sck.addEventListener("open", () => {
                resolve(sck);
            })
            sck.addEventListener("error", (err) => {
                reject(new Error(`Error during session initialisation!`, {cause: err}));
            });
        })
    }

    public static async Connect(host: string, bearer: string, timeout: number = 5000): Promise<CryoClientWebsocketSession> {
        const sid: UUID = crypto.randomUUID();

        const socket = await CryoClientWebsocketSession.ConstructSocket(host, timeout, bearer, sid);
        return new CryoClientWebsocketSession(host, sid, socket, timeout, bearer);
    }

    private async HandleWSError(err: Error) {
        this.log(`${err.name} Exception in CryoSocket: ${err.message}`);
        this.socket.close(CloseCode.CLOSE_SERVER_ERROR, `CryoSocket ${this.sid} was closed due to an error.`);
    }

    private TranslateCloseCode(code: number): string {
        switch (code as CloseCode) {
            case CloseCode.CLOSE_GRACEFUL:
                return "Connection closed normally.";
            case CloseCode.CLOSE_CLIENT_ERROR:
                return "Connection closed due to a client error.";
            case CloseCode.CLOSE_SERVER_ERROR:
                return "Connection closed due to a server error.";
            case CloseCode.CLOSE_CALE_MISMATCH:
                return "Connection closed due to a mismatch in client/server CALE configuration.";
            case CloseCode.CLOSE_CALE_HANDSHAKE:
                return "Connection closed due to an error in the CALE handshake.";
            default:
                return "Unspecified cause for connection closure."
        }
    }

    private inc_get_txid(): number {
        if (this.current_txid + 1 > 0xffffffff)
            this.current_txid = 0;

        return this.current_txid++;
    }

    private inc_get_ack(): number {
        if (this.current_ack + 1 > 0xffffffff)
            this.current_ack = 0;

        return this.current_ack++;
    }

    private async HandleClose(code: number, reason: Buffer) {
        this.log(`Websocket was closed. Code=${code} (${this.TranslateCloseCode(code)}), reason=${reason.toString("utf8")}.`);

        let current_attempt = 0;
        let back_off_delay = 5000;

        //If the connection was not normally closed, try to reconnect
        if (code !== CloseCode.CLOSE_GRACEFUL) {
            console.error(`Abnormal termination of Websocket connection, attempting to reconnect...`);
            ///@ts-expect-error
            this.socket = null;

            this.emit("disconnected", undefined)
            while (current_attempt < 5) {
                try {
                    this.socket = await CryoClientWebsocketSession.ConstructSocket(this.host, this.timeout, this.bearer, this.sid);
                    this.AttachListenersToSocket(this.socket);

                    this.emit("reconnected", undefined);
                    return;
                } catch (ex) {
                    if (ex instanceof Error) {
                        ///@ts-expect-error
                        const errorCode = ex.cause?.error?.code as string;
                        this.log(`Unable to reconnect to '${this.host}'. Error code: '${errorCode}'. Retry attempt in ${back_off_delay} ms. Attempt ${current_attempt++} / 5`);
                        await new Promise((resolve) => setTimeout(resolve, back_off_delay));
                        back_off_delay += current_attempt * 1000;
                    }
                }
            }
            this.log(`Gave up on reconnecting to '${this.host}'`);
        }

        if (this.socket)
            this.socket.close();

        this.emit("closed", [code, reason.toString("utf8")]);
    }

    private constructor(private host: string, private sid: UUID, private socket: WebSocket, private timeout: number, private bearer: string, private log: DebugLoggerFunction = CreateDebugLogger("CRYO_CLIENT_SESSION")) {
        super();
        this.AttachListenersToSocket(socket);
        setTimeout(() => this.emit("connected", undefined));
    }

    private AttachListenersToSocket(socket: WebSocket) {
        socket.addEventListener("message", async (msg: MessageEvent) => {
            if (msg.data instanceof ArrayBuffer)
                await this.routeFrame(new CryoBuffer(new Uint8Array(msg.data)));
        });

        socket.addEventListener("error", async (error_event) => {
            await this.HandleWSError(new Error("Unspecified WebSocket error!", {cause: error_event}));
        });

        socket.addEventListener("close", async (close_event) => {
            await this.HandleClose(close_event.code, new CryoBuffer((new TextEncoder().encode(close_event.reason))));
        });
    }

    private Destroy(code: number = 1000, message: string = "") {
        this.log(`Teardown of session. Code=${code}, reason=${message}`);
        this.socket.close(code, message);
    }

    /**
     * Route a frame of any kind to its corresponding handler
     * */
    private async routeFrame(frame: Buffer): Promise<void> {
        const type = BufferUtil.GetType(frame);
        this.bytes_rx += frame.byteLength;

        try {
            this.log(`IN ${CryoFrameInspector.Inspect(frame)}`);
        } catch {
            this.log(`IN <INVALID MESSAGE>`);
        }

        switch (type) {
            case BinaryMessageType.PING_PONG:
                await this.HandlePingPongMessage(frame);
                return;
            case BinaryMessageType.ERROR:
                await this.HandleErrorMessage(frame);
                return;
            case BinaryMessageType.ACK:
                await this.HandleAckMessage(frame);
                return;
            case BinaryMessageType.UTF8DATA:
                await this.HandleUTF8DataMessage(frame);
                return;
            case BinaryMessageType.BINARYDATA:
                await this.HandleBinaryDataMessage(frame);
                return;
            case BinaryMessageType.TX_START:
                await this.HandleTxStartMessage(frame);
                return;
            case BinaryMessageType.TX_CHUNK:
                await this.HandleTxChunkMessage(frame);
                return;
            case BinaryMessageType.TX_FINISH:
                await this.HandleTxFinishMessage(frame);
                return;
            default:
                this.log(`Unsupported binary message type ${type}!`);
        }
    }

    /**
     * Send a message to the server
     * */
    private async Send(outgoing_message: CryoBuffer): Promise<void> {
        if (!this.socket)
            return;

        if (this.socket.readyState === WebSocket.CLOSING || this.socket.readyState === WebSocket.CLOSED)
            return;

        //Create a pending message with a new ack number and queue it for acknowledgement by the server
        const type = BufferUtil.GetType(outgoing_message);
        if (type === BinaryMessageType.UTF8DATA || type === BinaryMessageType.BINARYDATA) {
            const message_ack = BufferUtil.GetAck(outgoing_message);
            this.server_ack_tracker.Track(message_ack, {
                timestamp: Date.now(),
                message: outgoing_message
            });
        }

        //Send the message buffer to the server
        try {
            ///@ts-ignore
            this.socket.send(outgoing_message.buffer);
        } catch (ex) {
            if (ex instanceof Error)
                await this.HandleWSError(ex);
        } finally {
            this.bytes_tx += outgoing_message.byteLength;
        }

        this.log(`OUT ${CryoFrameInspector.Inspect(outgoing_message)}`);
    }

    /**
     * Respond to PONG frames with PING and vice versa
     * */
    private async HandlePingPongMessage(message: CryoBuffer): Promise<void> {
        const decodedPingPongMessage = PingPongFrame
            .Deserialize(message);

        const ping_pongMessage = PingPongFrame
            .Serialize(this.sid, decodedPingPongMessage.ack, decodedPingPongMessage.payload === "pong" ? "ping" : "pong");

        await this.Send(ping_pongMessage);
    }

    /**
     * Handling of error messages from the server, currently just log it
     * */
    private async HandleErrorMessage(message: CryoBuffer): Promise<void> {
        const decodedErrorMessage = ErrorFrame
            .Deserialize(message);

        this.log(decodedErrorMessage.payload);
    }

    /**
     * ACK the pending message if it matches the server's ACK
     * */
    private async HandleAckMessage(message: Buffer): Promise<void> {
        const decodedAckMessage = ACKFrame
            .Deserialize(message);
        const ack_id = decodedAckMessage.ack;

        const found_message = this.server_ack_tracker.Confirm(ack_id);

        if (!found_message) {
            this.log(`Got unknown ack_id ${ack_id} from server.`);
            return;
        }

        this.messages_pending_server_ack.delete(ack_id);
        this.log(`Got ACK ${ack_id} from server.`);
    }

    /**
     * Extract payload from the binary message and emit the message event with the utf8 payload
     * */
    private async HandleUTF8DataMessage(message: Buffer): Promise<void> {
        const decodedDataMessage = Utf8DataFrame
            .Deserialize(message);

        const payload = decodedDataMessage.payload;

        const encodedAckMessage = ACKFrame
            .Serialize(this.sid, decodedDataMessage.ack);

        await this.Send(encodedAckMessage);
        this.emit("message-utf8", payload);
    }

    /**
     * Extract payload from the binary message and emit the message event with the binary payload
     * */
    private async HandleBinaryDataMessage(message: Buffer): Promise<void> {
        const decodedDataMessage = BinaryDataFrame
            .Deserialize(message);

        const payload = decodedDataMessage.payload;

        const encodedAckMessage = ACKFrame
            .Serialize(this.sid, decodedDataMessage.ack);

        await this.Send(encodedAckMessage);
        this.emit("message-binary", payload);
    }

    /**
     * Handle the start of a transaction
     * */
    private async HandleTxStartMessage(message: Buffer): Promise<void> {
        const decodedStartFrame = TXStartFrame
            .Deserialize(message);

        const ack_id = decodedStartFrame.ack;
        const encodedACKMessage = ACKFrame
            .Serialize(this.sid, ack_id);

        await this.Send(encodedACKMessage);

        let controller!: ReadableStreamDefaultController<Uint8Array>;
        const readable = new ReadableStream<Uint8Array>({
            start(c) {
                controller = c;
            },
            cancel: () => {
                this.streams.delete(decodedStartFrame.txId);
            }
        });

        this.streams.set(decodedStartFrame.txId, {
            controller,
            readable,
            name: decodedStartFrame.txName,
            claimed: false
        });

        this.emit("tx-start", [decodedStartFrame.txId, decodedStartFrame.txName]);
    }

    /**
     * Handle the end of a transaction
     * */
    private async HandleTxFinishMessage(message: Buffer): Promise<void> {
        const decodedFinishFrame = TXFinishFrame
            .Deserialize(message);

        const ack_id = decodedFinishFrame.ack;
        const encodedACKMessage = ACKFrame
            .Serialize(this.sid, ack_id);

        await this.Send(encodedACKMessage);

        const stream = this.streams.get(decodedFinishFrame.txId);
        if (!stream)
            return;

        stream.controller.close();
        if (!stream.claimed)
            setTimeout(() => {
                const currentStream = this.streams.get(decodedFinishFrame.txId);
                if (currentStream && !currentStream.claimed)
                    this.streams.delete(decodedFinishFrame.txId);

            }, this.STREAM_KEEP_TIMEOUT);
        this.emit("tx-finish", decodedFinishFrame.txId);
    }

    /**
     * Handle a transaction chunk
     * */
    private async HandleTxChunkMessage(message: Buffer): Promise<void> {
        const decodedChunkFrame = TXChunkFrame
            .Deserialize(message);

        //Handle stream
        if (!this.streams.has(decodedChunkFrame.txId))
            return;

        this.streams.get(decodedChunkFrame.txId)!.controller.enqueue(decodedChunkFrame.payload.buffer);

        this.emit("tx-chunk", [decodedChunkFrame.txId, decodedChunkFrame.payload]);
    }

    /**
     * Send an utf8 message to the server
     * */
    public async SendUTF8(message: string): Promise<void> {
        const new_ack_id = this.inc_get_ack();

        const formatted_message = Utf8DataFrame
            .Serialize(this.sid, new_ack_id, message);

        await this.Send(formatted_message);
    }

    /**
     * Send a binary message to the server
     * */
    public async SendBinary(message: CryoBuffer): Promise<void> {
        const new_ack_id = this.inc_get_ack();

        const formatted_message = BinaryDataFrame
            .Serialize(this.sid, new_ack_id, message);

        await this.Send(formatted_message);
    }

    /**
     * Send a ReadableStream to the server as a transaction
     * @param source The byte stream to send to the server
     * @param streamName Optionally, the name of the stream
     * @returns {Promise<void>} A Promise which will be resolved once the stream finishes
     * */
    public async Stream(source: ReadableStream<Uint8Array>, streamName: string = "anonymous"): Promise<void> {
        const start_ack_id = this.inc_get_ack();
        const new_txid = this.inc_get_txid();

        const start_frame = TXStartFrame.Serialize(this.sid, start_ack_id, new_txid, streamName);
        this.server_ack_tracker.Track(start_ack_id, {
            message: start_frame,
            timestamp: Date.now()
        });
        await this.Send(start_frame);

        const reader = source.getReader();

        try {
            while (true) {
                const {value, done} = await reader.read();
                if (done)
                    break;

                const chunk_frame = TXChunkFrame.Serialize(this.sid, new_txid, new CryoBuffer(value));
                await this.Send(chunk_frame);
            }
        } finally {
            reader.releaseLock();
        }

        const finish_ack_id = this.inc_get_ack();
        const finish_frame = TXFinishFrame.Serialize(this.sid, finish_ack_id, new_txid);
        this.server_ack_tracker.Track(finish_ack_id, {
            message: finish_frame,
            timestamp: Date.now()
        });
        await this.Send(finish_frame);
    }

    /**
     * Wait for a transaction from the server
     * @param streamName Optionally, the name of the stream to wait for. If left undefined, the first incoming stream will resolve
     * @param timeout Optionally, how long to wait for the server to start the transaction
     * @returns {Promise<ReadableStream<Uint8Array>>} A Promise which will be resolved with a {@link{ReadableStream}}
     * */
    public async WaitForStream(streamName: string = "anonymous", timeout: number = 1000): Promise<CryoStream<Uint8Array>> {
        const timeoutSig = AbortSignal.timeout(timeout);

        return new Promise<CryoStream<Uint8Array>>((resolve, reject) => {
            const onAbort = () => {
                this.off("tx-start", onTxStartListener);
                timeoutSig.removeEventListener("abort", onAbort);
                reject(new Error(`Timeout elapsed!`));
            }

            const cleanup = () => {
                this.off("tx-start", onTxStartListener);
                timeoutSig.removeEventListener("abort", onAbort);
            };

            const tryResolveStream = (txId: number, txName: string): boolean => {
                if (txName !== streamName)
                    return false;

                if (!this.streams.has(txId)) {
                    cleanup();
                    reject(new Error(`No stream id ${txId} present!`));
                    return true;
                }


                const stream = this.streams.get(txId)!;
                if (stream.claimed)
                    return false;

                stream.claimed = true;
                cleanup();

                resolve(new CryoStream(stream.readable, txId, (txIdToDelete) => this.streams.delete(txIdToDelete)));
                return true;
            };

            const onTxStartListener = (data: [txId: number, txName: string]) => {
                const [txId, txName] = data;
                tryResolveStream(txId, txName);
            };

            for (const [txId, stream] of this.streams.entries()) {
                if (stream.claimed)
                    continue;

                if (tryResolveStream(txId, stream.name))
                    return;
            }

            this.on("tx-start", onTxStartListener);
            timeoutSig.addEventListener("abort", onAbort);
        });
    }

    /**
     * Gracefully close the connection to the server
     * */
    public Close(): void {
        this.Destroy(CloseCode.CLOSE_GRACEFUL, "Client finished.");
    }

    /**
     * Getter for the internal cryo session id
     * */
    public get session_id(): UUID {
        return this.sid;
    }

    /**
     * Retrieve ewma RTT
     * */
    public get rtt() {
        return this.server_ack_tracker.rtt;
    }

    /**
     * Retrieve bytes transmitted
     * */
    public get tx() {
        return this.bytes_tx;
    }

    /**
     * Retrieve bytes received
     * */
    public get rx() {
        return this.bytes_rx;
    }
}
