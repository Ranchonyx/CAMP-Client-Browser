import { AckTracker } from "../Common/AckTracker/AckTracker.js";
import { CryoFrameInspector } from "../Common/CryoFrameInspector/CryoFrameInspector.js";
import { CreateDebugLogger } from "../Common/Util/CreateDebugLogger.js";
import { CryoBuffer } from "../Common/CryoBuffer/CryoBuffer.js";
import { CryoEventEmitter } from "../Common/CryoEventEmitter/CryoEventEmitter.js";
import { BufferUtil } from "../Common/Protocol/BufferUtil.js";
import { PingPongFrame } from "../Common/Protocol/Basic/PingPongFrame.js";
import { ErrorFrame } from "../Common/Protocol/Basic/ErrorFrame.js";
import { ACKFrame } from "../Common/Protocol/Basic/ACKFrame.js";
import { Utf8DataFrame } from "../Common/Protocol/Basic/Utf8DataFrame.js";
import { BinaryDataFrame } from "../Common/Protocol/Basic/BinaryDataFrame.js";
import { BinaryMessageType } from "../Common/Protocol/defs.js";
import { TXChunkFrame } from "../Common/Protocol/Transaction/TXChunkFrame.js";
import { TXFinishFrame } from "../Common/Protocol/Transaction/TXFinishFrame.js";
import { TXStartFrame } from "../Common/Protocol/Transaction/TXStartFrame.js";
var CloseCode;
(function (CloseCode) {
    CloseCode[CloseCode["CLOSE_GRACEFUL"] = 4000] = "CLOSE_GRACEFUL";
    CloseCode[CloseCode["CLOSE_CLIENT_ERROR"] = 4001] = "CLOSE_CLIENT_ERROR";
    CloseCode[CloseCode["CLOSE_SERVER_ERROR"] = 4002] = "CLOSE_SERVER_ERROR";
    CloseCode[CloseCode["CLOSE_CALE_MISMATCH"] = 4010] = "CLOSE_CALE_MISMATCH";
    CloseCode[CloseCode["CLOSE_CALE_HANDSHAKE"] = 4011] = "CLOSE_CALE_HANDSHAKE";
})(CloseCode || (CloseCode = {}));
/*
* Cryo Websocket session layer. Handles Binary formatting and ACKs and whatnot
* */
export class CryoClientWebsocketSession extends CryoEventEmitter {
    host;
    sid;
    socket;
    timeout;
    bearer;
    log;
    messages_pending_server_ack = new Map();
    server_ack_tracker = new AckTracker();
    streams = new Map();
    current_ack = 0;
    current_txid = 0;
    static async ConstructSocket(host, timeout, bearer, sid) {
        const full_host_url = new URL(host);
        full_host_url.searchParams.set("authorization", `Bearer ${bearer}`);
        full_host_url.searchParams.set("x-cryo-sid", sid);
        const sck = new WebSocket(full_host_url);
        sck.binaryType = "arraybuffer";
        return new Promise((resolve, reject) => {
            setTimeout(() => {
                if (sck.readyState !== WebSocket.OPEN)
                    reject(new Error(`Connection timeout of ${timeout} ms reached!`));
            }, timeout);
            sck.addEventListener("open", () => {
                resolve(sck);
            });
            sck.addEventListener("error", (err) => {
                reject(new Error(`Error during session initialisation!`, { cause: err }));
            });
        });
    }
    static async Connect(host, bearer, timeout = 5000) {
        const sid = crypto.randomUUID();
        const socket = await CryoClientWebsocketSession.ConstructSocket(host, timeout, bearer, sid);
        return new CryoClientWebsocketSession(host, sid, socket, timeout, bearer);
    }
    async HandleWSError(err) {
        this.log(`${err.name} Exception in CryoSocket: ${err.message}`);
        this.socket.close(CloseCode.CLOSE_SERVER_ERROR, `CryoSocket ${this.sid} was closed due to an error.`);
    }
    TranslateCloseCode(code) {
        switch (code) {
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
                return "Unspecified cause for connection closure.";
        }
    }
    inc_get_txid() {
        if (this.current_txid + 1 > 0xffffffff)
            this.current_txid = 0;
        return this.current_txid++;
    }
    inc_get_ack() {
        if (this.current_ack + 1 > 0xffffffff)
            this.current_ack = 0;
        return this.current_ack++;
    }
    async HandleClose(code, reason) {
        this.log(`Websocket was closed. Code=${code} (${this.TranslateCloseCode(code)}), reason=${reason.toString("utf8")}.`);
        let current_attempt = 0;
        let back_off_delay = 5000;
        //If the connection was not normally closed, try to reconnect
        console.error(`Abnormal termination of Websocket connection, attempting to reconnect...`);
        ///@ts-expect-error
        this.socket = null;
        this.emit("disconnected", undefined);
        while (current_attempt < 5) {
            try {
                this.socket = await CryoClientWebsocketSession.ConstructSocket(this.host, this.timeout, this.bearer, this.sid);
                this.AttachListenersToSocket(this.socket);
                this.emit("reconnected", undefined);
                return;
            }
            catch (ex) {
                if (ex instanceof Error) {
                    ///@ts-expect-error
                    const errorCode = ex.cause?.error?.code;
                    this.log(`Unable to reconnect to '${this.host}'. Error code: '${errorCode}'. Retry attempt in ${back_off_delay} ms. Attempt ${current_attempt++} / 5`);
                    await new Promise((resolve) => setTimeout(resolve, back_off_delay));
                    back_off_delay += current_attempt * 1000;
                }
            }
        }
        this.log(`Gave up on reconnecting to '${this.host}'`);
        if (this.socket)
            this.socket.close();
        this.emit("closed", [code, reason.toString("utf8")]);
    }
    constructor(host, sid, socket, timeout, bearer, log = CreateDebugLogger("CRYO_CLIENT_SESSION")) {
        super();
        this.host = host;
        this.sid = sid;
        this.socket = socket;
        this.timeout = timeout;
        this.bearer = bearer;
        this.log = log;
        this.AttachListenersToSocket(socket);
        setTimeout(() => this.emit("connected", undefined));
    }
    AttachListenersToSocket(socket) {
        socket.addEventListener("message", async (msg) => {
            if (msg.data instanceof ArrayBuffer)
                await this.routeFrame(new CryoBuffer(new Uint8Array(msg.data)));
        });
        socket.addEventListener("error", async (error_event) => {
            await this.HandleWSError(new Error("Unspecified WebSocket error!", { cause: error_event }));
        });
        socket.addEventListener("close", async (close_event) => {
            await this.HandleClose(close_event.code, new CryoBuffer((new TextEncoder().encode(close_event.reason))));
        });
    }
    Destroy(code = 1000, message = "") {
        this.log(`Teardown of session. Code=${code}, reason=${message}`);
        this.socket.close(code, message);
    }
    /*
    * Route a frame of any kind to its corresponding handler
    * */
    async routeFrame(frame) {
        const type = BufferUtil.GetType(frame);
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
    /*
    * Send a message to the server
    * */
    async Send(outgoing_message) {
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
        }
        catch (ex) {
            if (ex instanceof Error)
                await this.HandleWSError(ex);
        }
        this.log(`Sent ${CryoFrameInspector.Inspect(outgoing_message)} to server.`);
    }
    /*
    * Respond to PONG frames with PING and vice versa
    * */
    async HandlePingPongMessage(message) {
        const decodedPingPongMessage = PingPongFrame
            .Deserialize(message);
        const ping_pongMessage = PingPongFrame
            .Serialize(this.sid, decodedPingPongMessage.ack, decodedPingPongMessage.payload === "pong" ? "ping" : "pong");
        await this.Send(ping_pongMessage);
    }
    /*
    * Handling of error messages from the server, currently just log it
    * */
    async HandleErrorMessage(message) {
        const decodedErrorMessage = ErrorFrame
            .Deserialize(message);
        this.log(decodedErrorMessage.payload);
    }
    /*
    * ACK the pending message if it matches the server's ACK
    * */
    async HandleAckMessage(message) {
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
    /*
    * Extract payload from the binary message and emit the message event with the utf8 payload
    * */
    async HandleUTF8DataMessage(message) {
        const decodedDataMessage = Utf8DataFrame
            .Deserialize(message);
        const payload = decodedDataMessage.payload;
        const encodedAckMessage = ACKFrame
            .Serialize(this.sid, decodedDataMessage.ack);
        await this.Send(encodedAckMessage);
        this.emit("message-utf8", payload);
    }
    /*
    * Extract payload from the binary message and emit the message event with the binary payload
    * */
    async HandleBinaryDataMessage(message) {
        const decodedDataMessage = BinaryDataFrame
            .Deserialize(message);
        const payload = decodedDataMessage.payload;
        const encodedAckMessage = ACKFrame
            .Serialize(this.sid, decodedDataMessage.ack);
        await this.Send(encodedAckMessage);
        this.emit("message-binary", payload);
    }
    /*
    * Handle the start of a transaction
    * */
    async HandleTxStartMessage(message) {
        const decodedStartFrame = TXStartFrame
            .Deserialize(message);
        const ack_id = decodedStartFrame.ack;
        const encodedACKMessage = ACKFrame
            .Serialize(this.sid, ack_id);
        await this.Send(encodedACKMessage);
        let controller;
        const readable = new ReadableStream({
            start(c) {
                controller = c;
            },
            cancel: () => {
                this.streams.delete(decodedStartFrame.txId);
            }
        });
        this.streams.set(decodedStartFrame.txId, { controller, readable });
        this.emit("tx-start", [decodedStartFrame.txId, decodedStartFrame.txName]);
    }
    /*
    * Handle the end of a transaction
    * */
    async HandleTxFinishMessage(message) {
        const decodedFinishFrame = TXFinishFrame
            .Deserialize(message);
        const ack_id = decodedFinishFrame.ack;
        const encodedACKMessage = ACKFrame
            .Serialize(this.sid, ack_id);
        await this.Send(encodedACKMessage);
        //Handle stream
        if (!this.streams.has(decodedFinishFrame.txId))
            return;
        this.streams.get(decodedFinishFrame.txId).controller.close();
        this.streams.delete(decodedFinishFrame.txId);
        this.emit("tx-finish", decodedFinishFrame.txId);
    }
    /*
    * Handle a transaction chunk
    * */
    async HandleTxChunkMessage(message) {
        const decodedChunkFrame = TXChunkFrame
            .Deserialize(message);
        //Handle stream
        if (!this.streams.has(decodedChunkFrame.txId))
            return;
        this.streams.get(decodedChunkFrame.txId).controller.enqueue(decodedChunkFrame.payload.buffer);
        this.emit("tx-chunk", [decodedChunkFrame.txId, decodedChunkFrame.payload]);
    }
    /*
    * Send an utf8 message to the server
    * */
    async SendUTF8(message) {
        const new_ack_id = this.inc_get_ack();
        const formatted_message = Utf8DataFrame
            .Serialize(this.sid, new_ack_id, message);
        await this.Send(formatted_message);
    }
    /*
    * Send a binary message to the server
    * */
    async SendBinary(message) {
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
    async Stream(source, streamName = "anonymous") {
        const new_ack_id = this.inc_get_ack();
        const new_txid = this.inc_get_txid();
        const start_frame = TXStartFrame.Serialize(this.sid, new_ack_id, new_txid, streamName);
        await this.Send(start_frame);
        const reader = source.getReader();
        try {
            while (true) {
                const { value, done } = await reader.read();
                if (done)
                    break;
                const chunk_frame = TXChunkFrame.Serialize(this.sid, new_txid, new CryoBuffer(value));
                await this.Send(chunk_frame);
            }
        }
        finally {
            reader.releaseLock();
        }
        const finish_frame = TXFinishFrame.Serialize(this.sid, this.inc_get_ack(), new_txid);
        await this.Send(finish_frame);
    }
    /**
     * Wait for a transaction from the server
     * @param streamName Optionally, the name of the stream to wait for. If left undefined, the first incoming stream will resolve
     * @param timeout Optionally, how long to wait for the server to start the transaction
     * @returns {Promise<ReadableStream<Uint8Array>>} A Promise which will be resolved with a {@link{ReadableStream}}
     * */
    async WaitForStream(streamName = "anonymous", timeout = 1000) {
        const timeoutSig = AbortSignal.timeout(timeout);
        return new Promise((resolve, reject) => {
            const onTxStartListener = async (data) => {
                const [txId, txName] = data;
                if (txName === streamName) {
                    if (!this.streams.has(txId)) {
                        this.off("tx-start", onTxStartListener);
                        timeoutSig.removeEventListener("abort", onAbort);
                        reject(new Error(`No stream id ${txId} present!`));
                    }
                    const stream = this.streams.get(txId);
                    resolve(stream.readable);
                }
            };
            const onAbort = () => {
                this.off("tx-start", onTxStartListener);
                timeoutSig.removeEventListener("abort", onAbort);
                reject(new Error(`Timeout elapsed!`));
            };
            this.on("tx-start", onTxStartListener);
            timeoutSig.addEventListener("abort", onAbort);
        });
    }
    /**
     * Gracefully close the connection to the server
     * */
    Close() {
        this.Destroy(CloseCode.CLOSE_GRACEFUL, "Client finished.");
    }
    /**
     * Getter for the internal cryo session id
     * */
    get session_id() {
        return this.sid;
    }
}
