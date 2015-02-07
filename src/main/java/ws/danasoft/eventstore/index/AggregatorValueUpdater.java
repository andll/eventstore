package ws.danasoft.eventstore.index;

import com.google.common.collect.Iterables;
import ws.danasoft.eventstore.math.Aggregator;

import java.util.function.Function;

public class AggregatorValueUpdater<K extends Comparable<K>, V> implements Function<BTreeNode<K, V>, V> {
    private final Aggregator<V, V> aggregator;

    public AggregatorValueUpdater(Aggregator<V, V> aggregator) {
        this.aggregator = aggregator;
    }

    @Override
    public V apply(BTreeNode<K, V> node) {
        return aggregator.aggregate(Iterables.transform(node.getNodes(), BTreeNode::getValue));
    }
}
