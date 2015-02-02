package ws.danasoft.eventstore.index;

import com.google.common.base.Preconditions;

import java.util.function.Function;

public class BTreeNodeConfiguration<K extends Comparable<K>, V> {
    private final int maxBoundaries;
    private final Function<BTreeNode<K, V>, V> valueUpdater;

    public BTreeNodeConfiguration(int maxBoundaries, Function<BTreeNode<K, V>, V> valueUpdater) {
        Preconditions.checkArgument(maxBoundaries > 1, "maxBoundaries can not be less then 1");
        Preconditions.checkArgument(maxBoundaries % 2 == 0, "maxBoundaries must be odd");

        this.maxBoundaries = maxBoundaries;
        this.valueUpdater = valueUpdater;
    }

    public int getMaxBoundaries() {
        return maxBoundaries;
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
