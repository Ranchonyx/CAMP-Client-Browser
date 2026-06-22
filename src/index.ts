import {CAMPClientWebsocketSession} from "./CAMPClientWebsocketSession/CAMPClientWebsocketSession.js";

/**
 * Create a CAMP client
 * @param host - The server to connect to
 * @param bearer - The Bearer token for the server to validate
 * @param timeout - How long to wait until the client stops establishing a connection
 * */
export async function camp(host: string, bearer: string, timeout: number = 5000) {
    return CAMPClientWebsocketSession.Connect(host, bearer, timeout);
}