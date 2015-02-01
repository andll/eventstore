package ws.danasoft.eventstore.index;

import com.google.common.base.Preconditions;
import com.google.gson.stream.JsonWriter;

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Class is not thread safe
 */
public class BTreeNode<K extends Comparable<K>, V> {
    private static final boolean ASSERT_ENABLED = BTreeNode.class.desiredAssertionStatus();
    private final int maxBoundaries;
    private final List<K> boundaries;
    private final List<BTreeNode<K, V>> nodes;

    private Optional<K> key;
    private Optional<V> value;

    protected BTreeNode(int maxBoundaries, Optional<K> key, Optional<V> value) {
        this.key = key;
        this.value = value;
        this.maxBoundaries = maxBoundaries;
        this.boundaries = Collections.emptyList();
        this.nodes = Collections.emptyList();
        validate(maxBoundaries, boundaries, nodes);
    }

    protected BTreeNode(int maxBoundaries, List<K> boundaries, List<BTreeNode<K, V>> nodes) {
        this.key = Optional.empty();
        this.value = Optional.empty();
        validate(maxBoundaries, boundaries, nodes);
        this.maxBoundaries = maxBoundaries;
        this.boundaries = boundaries;
        this.nodes = nodes;
    }

    public static <K extends Comparable<K>, V> BTreeNode<K, V> emptyTree(int maxBoundaries) {
        Preconditions.checkArgument(maxBoundaries > 1, "maxBoundaries can not be less then 1");
        Preconditions.checkArgument(maxBoundaries % 2 == 0, "maxBoundaries must be odd");

        return new BTreeNode<K, V>(maxBoundaries, new ArrayList<>(boundariesCapacity(maxBoundaries)), new ArrayList<>(nodesCapacity(maxBoundaries)));
    }

    //1 more because our alg first adds new element and then splits
    private static int nodesCapacity(int maxBoundaries) {
        return maxBoundaries + 2;
    }

    private static int boundariesCapacity(int maxBoundaries) {
        return maxBoundaries + 1;
    }

    public final List<K> getBoundaries() {
        return Collections.unmodifiableList(boundaries);
    }

    public final List<BTreeNode<K, V>> getNodes() {
        return Collections.unmodifiableList(nodes);
    }

    //TODO optimisation for append
    public BTreeNode<K, V> add(K key, V value) {
        Optional<KeyNodePair<K, V>> o = _add(key, value);
        if (o.isPresent()) {
            BTreeNode<K, V> b = o.get().node;
            assert lowerKey().compareTo(b.lowerKey()) < 0 :
                    "Can not compose new node from keys " + lowerKey() + "," + b.lowerKey();
            ArrayList<K> newBoundaries = new ArrayList<>(boundariesCapacity(maxBoundaries));
            newBoundaries.add(o.get().key);
            ArrayList<BTreeNode<K, V>> newNodes = new ArrayList<>(nodesCapacity(maxBoundaries));
            newNodes.add(this);
            newNodes.add(b);
            return new BTreeNode<>(maxBoundaries, newBoundaries, newNodes);
        } else {
            return this;
        }
    }

    public Optional<KeyNodePair<K, V>> _add(K newKey, V newValue) {
        if (boundaries.isEmpty()) { //Leaf or empty node
            if (this.value.isPresent()) { //leaf
                K thisKey = this.key.get();
                if (thisKey.equals(newKey)) {
                    this.value = Optional.of(newValue);
                    return Optional.empty();
                }
                if (thisKey.compareTo(newKey) < 0) {
                    return Optional.of(new KeyNodePair<>(newKey, newLeaf(newKey, newValue)));
                }
                Optional<KeyNodePair<K, V>> r = Optional.of(new KeyNodePair<>(thisKey, newLeaf(thisKey, this.value.get())));
                this.key = Optional.of(newKey);
                this.value = Optional.of(newValue);
                return r;
            }
            //empty node
            this.key = Optional.of(newKey);
            this.value = Optional.of(newValue);
            return Optional.empty();
        }
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
        if (boundaries.size() > maxBoundaries) {
            assert boundaries.size() % 2 == 1;
            assert nodes.size() % 2 == 0;

            int middleIndex = boundaries.size() / 2 + 1;
            K middleKey = boundaries.get(middleIndex - 1);
            ArrayList<K> newBoundaries = new ArrayList<>(boundariesCapacity(maxBoundaries));
            newBoundaries.addAll(boundaries.subList(middleIndex, boundaries.size()));
            boundaries.subList(middleIndex - 1, boundaries.size()).clear();
            ArrayList<BTreeNode<K, V>> newNodes = new ArrayList<>(nodesCapacity(maxBoundaries));
            List<BTreeNode<K, V>> nodesLeftHalf = nodes.subList(nodes.size() / 2, nodes.size());
            newNodes.addAll(nodesLeftHalf);
            nodesLeftHalf.clear();
            result = Optional.of(new KeyNodePair<>(middleKey, new BTreeNode<K, V>(maxBoundaries, newBoundaries, newNodes)));
        } else {
            result = Optional.empty();
        }

        if (ASSERT_ENABLED) {
            try {
                validate(maxBoundaries, boundaries, nodes);
            } catch (IllegalArgumentException e) {
                throw new AssertionError(e.getMessage());
            }
        }

        return result;
    }

    private BTreeNode<K, V> newLeaf(K key, V value) {
        return new BTreeNode<>(maxBoundaries, Optional.of(key), Optional.of(value));
    }

    public boolean isLeaf() {
        return value.isPresent();
    }

    public K getKey() {
        return key.orElseThrow(() -> new IllegalStateException("Node is not a leaf"));
    }

    public V getValue() {
        return value.orElseThrow(() -> new IllegalStateException("Node is not a leaf"));
    }

    private K lowerKey() {
        return key.orElseGet(() -> boundaries.get(0));
    }

    private static <K extends Comparable<K>, V> void validate(int maxBoundaries, List<K> boundaries,
                                                              List<BTreeNode<K, V>> nodes) throws IllegalArgumentException {
        if (nodes.isEmpty() && boundaries.isEmpty()) {
            return;
        }
        if (boundaries.size() > maxBoundaries) {
            throw new IllegalArgumentException(String.format("Boundaries size %d is bigger then limit %d",
                    boundaries.size(), maxBoundaries));
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        BTreeNode bTreeNode = (BTreeNode) o;

        if (maxBoundaries != bTreeNode.maxBoundaries) return false;
        if (!boundaries.equals(bTreeNode.boundaries)) return false;
        if (!key.equals(bTreeNode.key)) return false;
        if (!nodes.equals(bTreeNode.nodes)) return false;
        if (!value.equals(bTreeNode.value)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = maxBoundaries;
        result = 31 * result + boundaries.hashCode();
        result = 31 * result + nodes.hashCode();
        result = 31 * result + key.hashCode();
        result = 31 * result + value.hashCode();
        return result;
    }

    public void printTo(JsonWriter jsonWriter) throws IOException {
        jsonWriter.beginObject();
        if (isLeaf()) {
            jsonWriter.name(key.get().toString()).value(value.get().toString());
        } else {
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
}
