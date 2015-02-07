package ws.danasoft.eventstore.index;

import java.util.Optional;

public interface BTreeSerializer<K, V> {
    int keySize();

    int valueSize();

    void writeKey(Optional<K> key, MappedRegion buffer);

    Optional<K> readKey(MappedRegion buffer);

    void writeValue(V value, MappedRegion buffer);

    V readValue(MappedRegion buffer);
}
