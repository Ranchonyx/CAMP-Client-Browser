export class CryoStream<T = Uint8Array> extends ReadableStream<T> {
    private firstChunkSize: number = -1;
    private receivedChunks: number = 0;

    public constructor(
        private source: ReadableStream<T>,
        public txId: number,
        public byteLength: number | null,
        private onDeleteStream: (txId: number) => void
    ) {
        super({
            start: async (controller) => {
                const reader = source.getReader();

                try {
                    while (true) {
                        const {value, done} = await reader.read();
                        if (done) {
                            controller.close();
                            this.onDeleteStream(this.txId);
                            return
                        }

                        controller.enqueue(value);
                        const sz = (value as Uint8Array).byteLength;
                        this.receivedChunks++;
                        if (this.firstChunkSize === -1)
                            this.firstChunkSize = sz;
                    }
                } catch (err) {
                    controller.error(err);
                    onDeleteStream(this.txId);
                } finally {
                    reader.releaseLock();
                }
            },
            cancel: async (reason) => {
                await this.source.cancel(reason).catch(() => {
                });
                onDeleteStream(this.txId);
            }
        });
    }

    public getRemainingChunks(): number | null {
        if (!this.byteLength)
            return null;

        const MAX_CHUNK = Math.ceil(this.byteLength / this.firstChunkSize);
        return MAX_CHUNK - this.receivedChunks;
    }
}