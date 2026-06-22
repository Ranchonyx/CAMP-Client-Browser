import {CAMPFrameInspector} from "../Common/CryoFrameInspector/CryoFrameInspector.js";
import {CreateDebugLogger, DebugLoggerFunction} from "../Common/Util/CreateDebugLogger.js";
import {ICryoClientWebsocketSessionEvents} from "./types/CryoClientWebsocketSession.js";
import {AckTracker} from "../Common/AckTracker/AckTracker.js";
import {CAMPEventEmitter} from "../Common/CryoEventEmitter/CAMPEventEmitter.js";
import {
    CAMPFrameType,
    BufferUtil,
    CAMPBuffer,
    CAMPNewId,
} from "camp-protocol";
import {CAMPBaseManager} from "./Namespaces/CAMP.Base.js";
import {CAMPTransactionManager} from "./Namespaces/CAMP.Transaction.js";

enum CloseCode {
    CLOSE_GRACEFUL = 4000,
    CLOSE_CLIENT_ERROR = 4001,
    CLOSE_SERVER_ERROR = 4002,
    CLOSE_CALE_MISMATCH = 4010,
    CLOSE_CALE_HANDSHAKE = 4011
}

/*
* Cryo Websocket session layer. Handles Binary formatting and ACKs and whatnot
* */
export class CAMPClientWebsocketSession extends CAMPEventEmitter<ICryoClientWebsocketSessionEvents> {
    private server_ack_tracker = new AckTracker();

    private bytes_tx = 0;
    private bytes_rx = 0;

    private current_ack = 0;
    private current_txid = 0;

    private receivedProtocolFeatures: bigint = 0n;

    public base: CAMPBaseManager;
    public stream: CAMPTransactionManager | null = null;

    private static async ConstructSocket(host: string, timeout: number, bearer: string, sid: bigint): Promise<WebSocket> {
        const full_host_url = new URL(host);
        full_host_url.searchParams.set("authorization", `Bearer ${bearer}`);
        full_host_url.searchParams.set("x-cryo-sid", String(sid));
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

    public static async Connect(host: string, bearer: string, timeout: number = 5000): Promise<CAMPClientWebsocketSession> {
        const sid = CAMPNewId();

        const socket = await CAMPClientWebsocketSession.ConstructSocket(host, timeout, bearer, sid);
        return new CAMPClientWebsocketSession(host, sid, socket, timeout, bearer);
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

    private async HandleClose(code: number, reason: CAMPBuffer) {
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
                    this.socket = await CAMPClientWebsocketSession.ConstructSocket(this.host, this.timeout, this.bearer, this.sid);
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

    private bind<T extends Function>(func: T): T {
        return func.bind(this);
    }

    private forwardMessageStrOrBuf(source: CAMPEventEmitter, event: keyof ICryoClientWebsocketSessionEvents) {
        source.on(event, (message) => this.emit(event, message));
    }

    private constructor(private host: string, private sid: bigint, private socket: WebSocket, private timeout: number, private bearer: string, private log: DebugLoggerFunction = CreateDebugLogger("CRYO_CLIENT_SESSION")) {
        super();

        this.base = new CAMPBaseManager(
            this.sid,
            this.bind(this.send),
            this.bind(this.inc_get_ack),
            this.bind(this.Destroy),
            (features) => this.receivedProtocolFeatures = features,
            this.server_ack_tracker
        );

        this.forwardMessageStrOrBuf(this.base, "message-binary");
        this.forwardMessageStrOrBuf(this.base, "message-utf8");
        this.forwardMessageStrOrBuf(this.base, "message-error");

        this.AttachListenersToSocket(socket);

        this.base.on("ready", () => {
            this.stream = new CAMPTransactionManager(
                this.sid,
                this.bind(this.send),
                this.bind(this.inc_get_ack),
                this.bind(this.inc_get_txid),
                this.bind(this.Destroy),
                () => this.receivedProtocolFeatures
            );
            this.emit("connected", undefined);
        });
    }

    private AttachListenersToSocket(socket: WebSocket) {
        socket.addEventListener("message", async (msg: MessageEvent) => {
            if (msg.data instanceof ArrayBuffer)
                await this.routeFrame(new CAMPBuffer(new Uint8Array(msg.data)));
        });

        socket.addEventListener("error", async (error_event) => {
            await this.HandleWSError(new Error("Unspecified WebSocket error!", {cause: error_event}));
        });

        socket.addEventListener("close", async (close_event) => {
            await this.HandleClose(close_event.code, new CAMPBuffer((new TextEncoder().encode(close_event.reason))));
        });
    }

    private Destroy(code: number = 1000, message: string = "") {
        this.log(`Teardown of session. Code=${code}, reason=${message}`);
        this.socket.close(code, message);
    }

    /**
     * Route a frame of any kind to its corresponding handler
     * */
    private async routeFrame(frame: CAMPBuffer): Promise<void> {
        const type = BufferUtil.GetType(frame);
        this.bytes_rx += frame.byteLength;

        try {
            this.log(`IN ${CAMPFrameInspector.Inspect(frame)}`);
        } catch {
            this.log(`IN <INVALID MESSAGE>`);
        }

        if (type >= CAMPFrameType.BINARYDATA && type <= CAMPFrameType.ENDPOINT_INFO)
            return this.base.handle(frame);

        if (type >= CAMPFrameType.TX_START && type <= CAMPFrameType.TX_CANCEL)
            return this.stream?.handle(frame);
    }

    /**
     * Send a message to the server
     * Resolves once the message has been ACK'd by the recipient
     * */
    private async send(outgoing_message: CAMPBuffer): Promise<void> {
        let ackPromise: PromiseWithResolvers<void> | null = null;

        if (!this.socket)
            return Promise.reject("No socket.");

        if (this.socket.readyState === WebSocket.CLOSING || this.socket.readyState === WebSocket.CLOSED)
            return Promise.reject("Invalid socket state.");

        //Create a pending message with a new ack number and queue it for acknowledgement by the server
        const type = BufferUtil.GetType(outgoing_message);
        if (
            type === CAMPFrameType.UTF8DATA ||
            type === CAMPFrameType.BINARYDATA ||
            type === CAMPFrameType.ERROR ||
            type === CAMPFrameType.ENDPOINT_INFO ||
            type === CAMPFrameType.TX_START ||
            type === CAMPFrameType.TX_FINISH ||
            type === CAMPFrameType.TX_FETCH
        ) {
            const message_ack = BufferUtil.GetAck(outgoing_message);
            ackPromise = Promise.withResolvers<void>();
            this.server_ack_tracker.Track(message_ack, {
                timestamp: Date.now(),
                message: outgoing_message,
                ackPromise
            });
        }

        //Send the message CryoBuffer to the server
        try {
            this.socket.send(outgoing_message.buffer);
        } catch (ex) {
            if (ex instanceof Error)
                await this.HandleWSError(ex);
        } finally {
            this.bytes_tx += outgoing_message.byteLength;
        }

        this.log(`OUT ${CAMPFrameInspector.Inspect(outgoing_message)}`);
        if (!ackPromise)
            return Promise.resolve();

        return ackPromise.promise;
    }

    /**
     * Gracefully close the connection to the server
     * */
    public Close(): void {
        this.Destroy(CloseCode.CLOSE_GRACEFUL, "Client finished.");
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
