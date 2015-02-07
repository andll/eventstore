package ws.danasoft.eventstore.index;

import java.util.Optional;

public class LongLongBTreeSerializer implements BTreeSerializer<Long, Long> {
    public static final long NO_VALUE = Long.MIN_VALUE;

    @Override
    public int keySize() {
        return 8;
    }

    @Override
    public int valueSize() {
        return 8;
    }

    @Override
    public void writeKey(Optional<Long> key, MappedRegion buffer) {
        if (key.isPresent()) {
            buffer.putLong(key.get());
        } else {
            buffer.putLong(NO_VALUE);
        }
    }

    @Override
    public Optional<Long> readKey(MappedRegion buffer) {
        return readOptionalLong(buffer);
    }

    @Override
    public void writeValue(Long value, MappedRegion buffer) {
        buffer.putLong(value);
    }

    @Override
    public Long readValue(MappedRegion buffer) {
        return buffer.getLong();
    }

    public static Optional<Long> readOptionalLong(MappedRegion buffer) {
        long l = buffer.getLong();
        if (l == NO_VALUE) {
            return Optional.empty();
        }
        return Optional.of(l);
    }
}
