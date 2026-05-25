export class CryoStream<T> extends ReadableStream<T> {
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
}