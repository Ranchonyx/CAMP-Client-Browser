export class CAMPReadable extends ReadableStream<Uint8Array> {
    private receivedBytes = 0n;
    private deleted = false;

    public constructor(
        private source: ReadableStream<Uint8Array>,
        public txId: number,
        public byteLength: bigint | null,
        private onDeleteStream: (txId: number) => void,
    ) {
        super({
            start: async (controller) => {
                const reader = source.getReader();

                try {
                    while (true) {
                        const {value, done} = await reader.read();

                        if (done) {
                            controller.close();
                            this.deleteOnce();
                            return;
                        }

                        this.receivedBytes += BigInt(value.byteLength);
                        controller.enqueue(value);
                    }
                } catch (err) {
                    controller.error(err);
                    this.deleteOnce();
                } finally {
                    reader.releaseLock();
                }
            },

            cancel: async (reason) => {
                await this.source.cancel(reason).catch(() => {
                });
                this.deleteOnce();
            },
        });
    }

    private deleteOnce() {
        if (this.deleted)
            return;

        this.deleted = true;
        this.onDeleteStream(this.txId);
    }

    public getReceivedBytes(): bigint {
        return this.receivedBytes;
    }

    public getRemainingBytes(): bigint | null {
        if (this.byteLength === null)
            return null;

        const remaining = this.byteLength - this.receivedBytes;
        return remaining > 0n ? remaining : 0n;
    }

    public isComplete(): boolean {
        return this.byteLength !== null && this.receivedBytes >= this.byteLength;
    }

    public getProgress(): number | null {
        if (this.byteLength === null || this.byteLength === 0n)
            return null;

        return Number(this.receivedBytes) / Number(this.byteLength);
    }
}