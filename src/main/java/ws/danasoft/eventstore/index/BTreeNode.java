package ws.danasoft.eventstore.index;

import com.google.common.base.Preconditions;
import com.google.gson.stream.JsonWriter;
import ws.danasoft.eventstore.storage.BlockStorage;

import java.io.IOException;
import java.io.StringWriter;
import java.lang.ref.Reference;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * Class is not thread safe
 */
public class BTreeNode<K extends Comparable<K>, V> {
    private final BTreeNodeConfiguration<K, V> configuration;
    private final BlockStorage blockStorage;
    private final MappedRegion buffer;
    private final long position;

    private final Lazy<Optional<K>> key;
    private final Lazy<V> value;
    private final Lazy<List<K>> boundaries;
    private final Lazy<List<BTreeNode<K, V>>> nodes;

    protected BTreeNode(BTreeNodeConfiguration<K, V> configuration, BlockStorage blockStorage, MappedRegion buffer, long position) {
        this.blockStorage = blockStorage;
        this.buffer = buffer;
        this.configuration = configuration;
        this.position = position;
        key = new Lazy<>(() -> {
            buffer.position(configuration.keyPosition());
            return configuration.getSerializer().readKey(buffer);
        });
        value = new Lazy<>(() -> {
            buffer.position(configuration.valuePosition());
            return configuration.getSerializer().readValue(buffer);
        });
        boundaries = new Lazy<>(this::_getBoundaries);
        nodes = new Lazy<>(this::_getNodes);
    }

    /**
     * assumes {@link BTreeNodeConfiguration#keyPosition()} is 0
     */
    protected static <K extends Comparable<K>, V> BTreeNode<K, V> map(BTreeNodeConfiguration<K, V> configuration, BlockStorage blockStorage, long position) {
        MappedRegion region;
        if (blockStorage.getLong(position) == LongLongBTreeSerializer.NO_VALUE) {
            region = blockStorage.mapRegion(position, configuration.nodeSize());
        } else {
            region = blockStorage.mapRegion(position, configuration.leafSize());
        }
        return new BTreeNode<>(configuration, blockStorage, region, position);
    }

    /**
     * assumes {@link ws.danasoft.eventstore.storage.FseekBlockStorage#mapRegion(long, int)} returns buffer with position 0
     * assumes {@link BTreeNodeConfiguration#keyPosition()} is 0
     */
    protected static <K extends Comparable<K>, V> BTreeNode<K, V> allocateNode(BTreeNodeConfiguration<K, V> configuration, BlockStorage blockStorage) {
        int size = configuration.nodeSize();
        long position = blockStorage.allocateRegion(size);
        MappedRegion region = blockStorage.mapRegion(position, size);
        BTreeSerializer<K, V> serializer = configuration.getSerializer();
        serializer.writeKey(Optional.empty(), region);
        region.putLong(configuration.boundariesPosition(), LongLongBTreeSerializer.NO_VALUE);
        region.putLong(configuration.nodesPosition(), LongLongBTreeSerializer.NO_VALUE);
        region.flush();
        return new BTreeNode<>(configuration, blockStorage, region, position);
    }

    /**
     * assumes {@link ws.danasoft.eventstore.storage.FseekBlockStorage#mapRegion(long, int)} returns buffer with position 0
     * assumes {@link BTreeNodeConfiguration#keyPosition()} is 0
     * assumes {@link BTreeNodeConfiguration#valuePosition()} is keyPosition + valueSize
     */
    protected static <K extends Comparable<K>, V> BTreeNode<K, V> allocateLeaf(BTreeNodeConfiguration<K, V> configuration, BlockStorage blockStorage, K key, V value) {
        int size = configuration.leafSize();
        long position = blockStorage.allocateRegion(size);
        MappedRegion region = blockStorage.mapRegion(position, size);
        BTreeSerializer<K, V> serializer = configuration.getSerializer();
        serializer.writeKey(Optional.of(key), region);
        serializer.writeValue(value, region);
        region.flush();
        return new BTreeNode<>(configuration, blockStorage, region, position);
    }

    public final List<K> getBoundaries() {
        return Collections.unmodifiableList(boundaries.get());
    }

    private List<K> _getBoundaries() {
        Preconditions.checkState(!isLeaf(), "Can not get boundaries for leaf");
        List<K> boundaries = new ArrayList<>(configuration.boundariesCapacity());
        buffer.position(configuration.boundariesPosition());
        BTreeSerializer<K, V> serializer = configuration.getSerializer();
        Optional<K> key;
        while ((key = serializer.readKey(buffer)).isPresent()) {
            boundaries.add(key.get());
            if (boundaries.size() == configuration.boundariesCapacity()) {
                break;
            }
        }
        return boundaries;
    }

    public final List<BTreeNode<K, V>> getNodes() {
        return Collections.unmodifiableList(nodes.get());
    }

    private List<BTreeNode<K, V>> _getNodes() {
        Preconditions.checkState(!isLeaf(), "Can not get nodes for leaf");
        List<BTreeNode<K, V>> nodes = new ArrayList<>(configuration.nodesCapacity());
        buffer.position(configuration.nodesPosition());
        Optional<Long> reference;
        while ((reference = LongLongBTreeSerializer.readOptionalLong(buffer)).isPresent()) {
            nodes.add(map(configuration, blockStorage, reference.get()));
            if (nodes.size() == configuration.nodesCapacity()) {
                break;
            }
        }
        return nodes;
    }

    //TODO getNodes(i) should be == getNodes().get(i) for writable nodes
    public BTreeNode<K, V> getNode(int index) {
        buffer.position(configuration.nodePosition(index));
        Optional<Long> reference = LongLongBTreeSerializer.readOptionalLong(buffer);
        return map(configuration, blockStorage, reference.get());
    }

    BTreeNode<K, V> add(K key, V value) {
        Optional<KeyNodePair<K, V>> o = _add(key, value);
        if (!o.isPresent()) {
            return this;
        }
        BTreeNode<K, V> b = o.get().node;
        assert lowerKey().compareTo(b.lowerKey()) < 0 :
                "Can not compose new node from keys " + lowerKey() + "," + b.lowerKey();
        BTreeNode<K, V> node = allocateNode(configuration, blockStorage);
        BTreeSerializer<K, V> serializer = configuration.getSerializer();
        node.buffer.position(configuration.boundariesPosition());
        serializer.writeKey(Optional.of(o.get().key), node.buffer);
        serializer.writeKey(Optional.empty(), node.buffer);
        node.buffer.position(configuration.nodesPosition());
        node.buffer.putLong(this.position);
        node.buffer.putLong(b.position);
        node.buffer.putLong(LongLongBTreeSerializer.NO_VALUE);
        node.buffer.flush();
        return node;
    }

    //TODO optimisation for append
    private Optional<KeyNodePair<K, V>> _add(K newKey, V newValue) {
        if (isLeaf()) {
            K thisKey = key.get().get();
            if (thisKey.equals(newKey)) {
                setValue(newValue);
                return Optional.empty();
            }
            if (thisKey.compareTo(newKey) < 0) {
                return Optional.of(new KeyNodePair<>(newKey, newLeaf(newKey, newValue)));
            }
            Optional<KeyNodePair<K, V>> r = Optional.of(new KeyNodePair<>(thisKey, newLeaf(thisKey, value.get())));
            setKey(newKey);
            setValue(newValue);
            return r;
        }
        List<K> boundaries = this.boundaries.get();
        List<BTreeNode<K, V>> nodes = this.nodes.get();
        int keySearch = Collections.binarySearch(boundaries, newKey);
        int nodePosition;
        if (keySearch < 0) {
            nodePosition = -(keySearch + 1);
        } else {
            nodePosition = keySearch + 1;
        }

        BTreeNode<K, V> currentNode = nodes.get(nodePosition);
        Optional<KeyNodePair<K, V>> knp = currentNode._add(newKey, newValue);
        if (!knp.isPresent()) {
            nodeChanged();
            return Optional.empty();
        }

        BTreeNode<K, V> newNode = knp.get().node;
        if (keySearch < 0) {
            K separatingKey = knp.get().key;
            if (separatingKey.compareTo(newKey) > 0) {
                boundaries.add(nodePosition, separatingKey);
                nodes.add(nodePosition + 1, newNode);
            } else {
                boundaries.add(nodePosition, separatingKey);
                nodes.add(nodePosition + 1, newNode);
            }
        } else {
            nodes.set(nodePosition, newNode);
        }

        Optional<KeyNodePair<K, V>> result;
        if (boundaries.size() > configuration.getMaxBoundaries()) {
            assert boundaries.size() % 2 == 1;
            assert nodes.size() % 2 == 0;

            int middleIndex = boundaries.size() - 1;
            K middleKey = boundaries.get(middleIndex - 1);
            ArrayList<K> newBoundaries = new ArrayList<>(configuration.boundariesCapacity());
            newBoundaries.addAll(boundaries.subList(middleIndex, boundaries.size()));
            boundaries.subList(middleIndex - 1, boundaries.size()).clear();
            ArrayList<BTreeNode<K, V>> newNodes = new ArrayList<>(configuration.nodesCapacity());
            List<BTreeNode<K, V>> nodesLeftHalf = nodes.subList(middleIndex, nodes.size());
            newNodes.addAll(nodesLeftHalf);
            nodesLeftHalf.clear();
            BTreeNode<K, V> splitTo = allocateNode(configuration, blockStorage);
            splitTo.boundaries.reset(newBoundaries);
            splitTo.nodes.reset(newNodes);
            splitTo.flushNodesAndBoundaries(newBoundaries, newNodes);
            result = Optional.of(new KeyNodePair<>(middleKey, splitTo));
        } else {
            result = Optional.empty();
        }

        flushNodesAndBoundaries(boundaries, nodes);

        nodeChanged();
        return result;
    }

    private void flushNodesAndBoundaries(List<K> boundaries, List<BTreeNode<K, V>> nodes) {
        BTreeSerializer<K, V> serializer = configuration.getSerializer();

        int i;
        buffer.position(configuration.boundariesPosition());
        for (i = 0; i < boundaries.size(); i++) {
            serializer.writeKey(Optional.of(boundaries.get(i)), buffer);
        }
        if (i < configuration.boundariesCapacity()) {
            serializer.writeKey(Optional.empty(), buffer);
        }

        buffer.position(configuration.nodesPosition());
        for (i = 0; i < nodes.size(); i++) {
            buffer.putLong(nodes.get(i).position);
        }
        if (i < configuration.nodesCapacity()) {
            buffer.putLong(LongLongBTreeSerializer.NO_VALUE);
        }
        buffer.flush();
    }

    private BTreeNode<K, V> newLeaf(K key, V value) {
        return allocateLeaf(configuration, blockStorage, key, value);
    }

    public boolean isLeaf() {
        return key.get().isPresent();
    }

    public K getKey() {
        return key.get().orElseThrow(() -> new IllegalStateException("Node is not a leaf"));
    }

    public V getValue() {
        return value.get();
    }

    private K lowerKey() {
        return key.get().orElseGet(() -> boundaries.get().get(0));
    }

    private static <K extends Comparable<K>, V> void validate(BTreeNodeConfiguration configuration, List<K> boundaries,
                                                              List<BTreeNode<K, V>> nodes) throws IllegalArgumentException {
        if (boundaries.size() > configuration.getMaxBoundaries()) {
            throw new IllegalArgumentException(String.format("Boundaries size %d is bigger then limit %d",
                    boundaries.size(), configuration.getMaxBoundaries()));
        }
        if (nodes.size() != boundaries.size() + 1) {
            throw new IllegalArgumentException(String.format("Nodes size %d is illegal with boundaries size %d",
                    nodes.size(), boundaries.size()));
        }
        K prevBoundary = null;
        for (int i = 0; i < boundaries.size(); i++) {
            K boundary = boundaries.get(i);
            if (prevBoundary != null) {
                if (boundary.compareTo(prevBoundary) <= 0) {
                    throw new IllegalArgumentException(String.format("Boundary at %d %s is lower or equal then previous boundary %s",
                            i, boundary, prevBoundary));
                }
            }
            prevBoundary = boundary;
        }
    }

    private void nodeChanged() {
        assert !isLeaf();

        setValue(configuration.getValueUpdater().apply(this));
    }

    private void setValue(V v) {
        buffer.position(configuration.valuePosition());
        configuration.getSerializer().writeValue(v, buffer);
        buffer.flush();
        value.reset(v);
    }

    private void setKey(K k) {
        Optional<K> newKey = Optional.of(k);
        buffer.position(configuration.keyPosition());
        configuration.getSerializer().writeKey(newKey, buffer);
        buffer.flush();
        key.reset(newKey);
    }

    public void printTo(JsonWriter jsonWriter) throws IOException {
        List<BTreeNode<K, V>> nodes = this.nodes.get();
        List<K> boundaries = this.boundaries.get();
        jsonWriter.beginObject();
        V value = this.value.get();
        jsonWriter.name("_position").value(position);
        if (isLeaf()) {
            Optional<K> key = this.key.get();
            jsonWriter.name(key.get().toString()).value(value.toString());
        } else {
            jsonWriter.name("_agg").value(value.toString());
            for (int i = 0; i < nodes.size(); i++) {
                if (i == nodes.size() - 1) {
                    jsonWriter.name(">=" + boundaries.get(nodes.size() - 2));
                } else {
                    jsonWriter.name("<" + boundaries.get(i));
                }
                nodes.get(i).printTo(jsonWriter);
            }
        }
        jsonWriter.endObject();
    }

    @Override
    public String toString() {
        StringWriter out = new StringWriter();
        try {
            try (JsonWriter jsonWriter = new JsonWriter(out)) {
                jsonWriter.setIndent(" ");
                printTo(jsonWriter);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return out.toString();
    }

    private static class KeyNodePair<K extends Comparable<K>, V> {
        private final K key;
        private final BTreeNode<K, V> node;

        public KeyNodePair(K key, BTreeNode<K, V> node) {
            this.key = key;
            this.node = node;
        }
    }

    long getPosition() {
        return position;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        BTreeNode bTreeNode = (BTreeNode) o;

        if (position != bTreeNode.position) return false;
        if (!configuration.equals(bTreeNode.configuration)) return false;
        if (!blockStorage.equals(bTreeNode.blockStorage)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = configuration.hashCode();
        result = 31 * result + blockStorage.hashCode();
        result = 31 * result + (int) (position ^ (position >>> 32));
        return result;
    }

    private static class Lazy<T> {
        private final Supplier<T> supplier;
        private Reference<T> t = newReference(null);

        public Lazy(Supplier<T> supplier) {
            this.supplier = supplier;
        }

        public T get() {
            T _t = t.get();
            if (_t != null) {
                return _t;
            }
            _t = supplier.get();
            t = newReference(_t);
            return _t;
        }

        public void reset(T _t) {
            t = newReference(_t);
        }

        private Reference<T> newReference(T _t) {
            return new WeakReference<>(_t);
        }
    }
}
