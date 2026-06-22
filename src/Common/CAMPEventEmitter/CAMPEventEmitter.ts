import Guard from "../Util/Guard.js";

type Listener<T> = (payload: T) => void;

export class CAMPEventEmitter<EventMap extends Record<string, any> = Record<string, any>> {
    private target = new EventTarget();

    private listeners = new Map<
        keyof EventMap,
        Map<Listener<any>, EventListener>
    >();

    public on<K extends keyof EventMap>(
        type: K,
        listener: Listener<EventMap[K]>
    ) {
        Guard.CastAs<string>(type);

        const wrappedListener: EventListener = (e: Event) => {
            listener((e as CustomEvent<EventMap[K]>).detail);
        };

        let typeListeners = this.listeners.get(type);

        if (!typeListeners) {
            typeListeners = new Map();
            this.listeners.set(type, typeListeners);
        }

        typeListeners.set(listener, wrappedListener);

        this.target.addEventListener(type as string, wrappedListener);
    }

    public off<K extends keyof EventMap>(
        type: K,
        listener: Listener<EventMap[K]>
    ) {
        Guard.CastAs<string>(type);

        const typeListeners = this.listeners.get(type);
        const wrappedListener = typeListeners?.get(listener);

        if (!wrappedListener) return;

        this.target.removeEventListener(type as string, wrappedListener);

        typeListeners!.delete(listener);

        if (typeListeners!.size === 0) {
            this.listeners.delete(type);
        }
    }

    public emit<K extends keyof EventMap>(
        type: K,
        payload: EventMap[K]
    ) {
        Guard.CastAs<string>(type);

        this.target.dispatchEvent(
            new CustomEvent(String(type), {
                detail: payload,
            })
        );
    }
}