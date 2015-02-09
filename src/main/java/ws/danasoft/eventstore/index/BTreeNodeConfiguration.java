package ws.danasoft.eventstore.index;

import com.google.common.base.Preconditions;

import java.util.function.Function;

public class BTreeNodeConfiguration<K extends Comparable<K>, V> {
    private final int maxBoundaries;
    private final Function<BTreeNode<K, V>, V> valueUpdater;
    private final BTreeSerializer<K, V> serializer;

    public BTreeNodeConfiguration(int maxBoundaries, Function<BTreeNode<K, V>, V> valueUpdater, BTreeSerializer<K, V> serializer) {
        this.serializer = serializer;
        Preconditions.checkArgument(maxBoundaries > 1, "maxBoundaries can not be less then 1");
        Preconditions.checkArgument(maxBoundaries % 2 == 0, "maxBoundaries must be odd");

        this.maxBoundaries = maxBoundaries;
        this.valueUpdater = valueUpdater;
    }

    public int nodeSize() {
        int keySize = serializer.keySize();
        int valueSize = serializer.valueSize();
        int referenceSize = nodeReferenceSize();
        return keySize + valueSize + keySize * (boundariesCapacity()) + referenceSize * nodesCapacity();
    }

    public int leafSize() {
        int keySize = serializer.keySize();
        int valueSize = serializer.valueSize();
        return keySize + valueSize;
    }

    public int keyPosition() {
        return 0;
    }

    public int valuePosition() {
        return serializer.keySize();
    }

    public int boundariesPosition() {
        int keySize = serializer.keySize();
        int valueSize = serializer.valueSize();
        return keySize + valueSize;
    }

    public int nodesPosition() {
        int keySize = serializer.keySize();
        int valueSize = serializer.valueSize();
        return keySize + valueSize + keySize * (boundariesCapacity());
    }

    public int nodePosition(int index) {
        int keySize = serializer.keySize();
        int valueSize = serializer.valueSize();
        int referenceSize = nodeReferenceSize();
        return keySize + valueSize + keySize * (boundariesCapacity()) + index * referenceSize;
    }

    private int nodeReferenceSize() {
        return 8;
    }

    public int boundariesCapacity() {
        return getMaxBoundaries() + 1;
    }

    //1 more because our alg first adds new element and then splits
    public int nodesCapacity() {
        return getMaxBoundaries() + 2;
    }

    public int getMaxBoundaries() {
        return maxBoundaries;
    }

    public BTreeSerializer<K, V> getSerializer() {
        return serializer;
    }

    public Function<BTreeNode<K, V>, V> getValueUpdater() {
        return valueUpdater;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        BTreeNodeConfiguration that = (BTreeNodeConfiguration) o;

        if (maxBoundaries != that.maxBoundaries) return false;
        if (!valueUpdater.equals(that.valueUpdater)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = maxBoundaries;
        result = 31 * result + valueUpdater.hashCode();
        return result;
    }
}
