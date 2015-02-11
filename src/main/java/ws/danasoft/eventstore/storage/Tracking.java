package ws.danasoft.eventstore.storage;

public class Tracking<T> {
    private T value;
    private boolean changed;

    private Tracking(T value) {
        this.value = value;
    }

    public static <T> Tracking<T> create(T t) {
        return new Tracking<>(t);
    }

    public void setValue(T newValue) {
        value = newValue;
        changed = true;
    }

    public T getValue() {
        return value;
    }

    public boolean isChanged() {
        return changed;
    }
}
