export class CryoBuffer {
    buffer;
    view;
    constructor(buffer) {
        this.buffer = buffer;
        this.view = new DataView(buffer.buffer, buffer.byteOffset, buffer.byteLength);
    }
    static alloc(length) {
        return new CryoBuffer(new Uint8Array(length));
    }
    static from(input, encoding) {
        if (encoding === "utf8")
            return new CryoBuffer(new TextEncoder().encode(input));
        const data = new Uint8Array(input.length / 2);
        for (let i = 0; i < data.length; i++)
            data[i] = parseInt(input.substring(i * 2, i * 2 + 2), 16);
        return new CryoBuffer(data);
    }
    static concat(buffers) {
        if (buffers.length === 0)
            return CryoBuffer.alloc(0);
        const length_total = buffers.reduce((acc, v) => acc + v.byteLength, 0);
        const result = new Uint8Array(length_total);
        let offset = 0;
        for (const buf of buffers) {
            result.set(buf.buffer, offset);
            offset += buf.byteLength;
        }
        return new CryoBuffer(result);
    }
    writeUint32BE(value, offset) {
        this.view.setUint32(offset, value);
    }
    writeUint8(value, offset) {
        this.view.setUint8(offset, value);
    }
    readUint32BE(offset) {
        return this.view.getUint32(offset);
    }
    readUint8(offset) {
        return this.view.getUint8(offset);
    }
    write(text, offset = 0) {
        this.buffer.set(new TextEncoder().encode(text), offset);
    }
    set(buffer, offset) {
        this.buffer.set(buffer.buffer, offset);
    }
    toString(encoding) {
        if (encoding === "utf8")
            return new TextDecoder().decode(this.buffer);
        return [...this.buffer]
            .map(byte => byte.toString(16).padStart(2, "0"))
            .join("");
    }
    subarray(start, end) {
        return new CryoBuffer(this.buffer.subarray(start, end));
    }
    copy(target, target_start = 0) {
        target.buffer.set(this.buffer, target_start);
    }
    get byteLength() {
        return this.byteLength;
    }
}
