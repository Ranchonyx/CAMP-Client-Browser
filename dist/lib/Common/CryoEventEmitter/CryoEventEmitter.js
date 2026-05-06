import Guard from "../Util/Guard.js";
export class CryoEventEmitter {
    target = new EventTarget();
    listeners = new Map();
    on(type, listener) {
        Guard.CastAs(type);
        const wrappedListener = (e) => {
            listener(e.detail);
        };
        let typeListeners = this.listeners.get(type);
        if (!typeListeners) {
            typeListeners = new Map();
            this.listeners.set(type, typeListeners);
        }
        typeListeners.set(listener, wrappedListener);
        this.target.addEventListener(type, wrappedListener);
    }
    off(type, listener) {
        Guard.CastAs(type);
        const typeListeners = this.listeners.get(type);
        const wrappedListener = typeListeners?.get(listener);
        if (!wrappedListener)
            return;
        this.target.removeEventListener(type, wrappedListener);
        typeListeners.delete(listener);
        if (typeListeners.size === 0) {
            this.listeners.delete(type);
        }
    }
    emit(type, payload) {
        Guard.CastAs(type);
        this.target.dispatchEvent(new CustomEvent(String(type), {
            detail: payload,
        }));
    }
}
