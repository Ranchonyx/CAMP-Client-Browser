# Cryo-Client-browser

#### Part of the Cryo Ecosystem

```
 █████ ██████  ██   ██  █████  
██     ██   ██  ██ ██  ██   ██ 
██     ██████    ███   ██   ██ 
██     ██ ██     ██    ██   ██ 
██     ██  ██    ██    ██   ██ 
 █████ ██   ██   ██     █████  
                Browser client implementation
```

---

## Cryo / Overview

Cryo is a lightweight, efficient Websocket framework intended for building real-time systems

Client implementations are available for:

- **TypeScript / JavaScript** under **Node.Js**
- **TypeScript / JavaScript** under **modern Browsers**
- **C#** under **.NET 8.0**

A server implementation is available for **TypeScript / JavaScript** under **Node.Js**

## Cryo-Client-Browser / Overview

The Cryo Browser client takes care of the following:

- Authentication at the server
- Correct framing and structuring of received and sent data

The client provides a public API for creating and destroying an instance.
It provides access incoming communication via events

## Setup

To set up a Cryo Client, simply import the ``cryo`` function from the ``cryo-client-browser`` package.

The ``cryo``-function takes two arguments:

- host
    - a required host string
- bearer
    - a required authentication token string
- timeout
    - an optional timeout value, indicating how long the client should wait until a connection request to the server is
      aborted

## CryoClientWebsocketSession / Overview

### Public methods

| Name          | Parameter                                   | Description                                | Returns                 |
|---------------|---------------------------------------------|--------------------------------------------|-------------------------|
| SendUTF8      | message: string                             | Sends an UTF8 string to the server         | Promise<void>           |
| SendBinary    | message: CryoBuffer                         | Send arbitrary binary data to the server   | Promise<void>           |
| Stream        | source: ReadableStream, streamName?: string | Stream a ReadableStream to the server      | Promise<void>           |
| WaitForStream | streamName?: string, timeout?: number       | Wait for a named stream from the server    | Promise<ReadableStream> |
| Close         |                                             | Closes the underlying Websocket connection | Promise<void>           |

### Data Events

These events are emitted when the server-side session receives data from a client-side session

| Name           | Parameter                      | Description                                                    |
|----------------|--------------------------------|----------------------------------------------------------------|
| message-utf8   | string                         | Emitted, when the session receives a utf8 text message         |
| message-binary | CryoBuffer                     | Emitted, when the session receives an arbitrary binary message |

### Meta events

This category of events is emitted when the session state changes

| Name         | Parameter                      | Description                                     |
|--------------|--------------------------------|-------------------------------------------------|
| connected    |                                | Emitted, when the session successfully connects |
| disconnected |                                | Emitted, when the session has been disconnected |
| reconnected  |                                | Emitted, when the session has reconnected       |
| closed       | [code: number, reason: string] | Emitted, when the session is closed             |

## Cryo-Client / Example

```typescript
import {cryo} from "cryo-client-browser";

const HOST = "localhost:8080";
const TOKEN = "SOME_AUTH_TOKEN";

const client = await cryo(HOST, TOKEN, 10000);
client.on("connected", () => {
    console.info(`Successfully connected to ${HOST}`);
});
```