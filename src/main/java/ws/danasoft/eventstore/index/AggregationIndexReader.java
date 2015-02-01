package ws.danasoft.eventstore.index;

import com.google.common.base.Function;
import com.google.common.collect.BoundType;
import com.google.common.collect.Iterables;
import com.google.common.collect.Range;
import ws.danasoft.eventstore.math.Aggregator;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class AggregationIndexReader<R, A extends Comparable<A>> {
    private final IndexNode<R, A> root;
    private final Aggregator<R, A> aggregator;

    public AggregationIndexReader(IndexNode<R, A> root, Aggregator<R, A> aggregator) {
        this.root = root;
        this.aggregator = aggregator;
    }

    public R evaluate(Range<A> range) {
        return evaluate(range, root);
    }

    private R evaluate(Range<A> range, IndexNode<R, A> node) {
        List<R> iterable = new ArrayList<>(node.nodes().size());
        List<A> boundaries = node.boundaries();
        A prevBoundary = null;
        //TODO nodes with single value
        for (int i = 0; i < boundaries.size(); i++) {
            A boundary = boundaries.get(i);
            Range<A> currentRange;
            if (prevBoundary == null) {
                currentRange = Range.lessThan(boundary);
            } else {
                currentRange = Range.range(prevBoundary, BoundType.CLOSED, boundary, BoundType.OPEN);
            }
            IndexNode<R, A> child = node.nodes().get(i);
            processRange(range, iterable, currentRange, child);
            prevBoundary = boundary;
        }
        if (prevBoundary != null) {
            Range<A> currentRange = Range.atLeast(prevBoundary);
            IndexNode<R, A> child = node.nodes().get(boundaries.size());
            processRange(range, iterable, currentRange, child);
        }

        return aggregator.aggregate(iterable);
//                AggregationIndexReader.transformAndFilter(
//                        node.nodeMap().entrySet(),
//                        new Function<Map.Entry<Range<A>, ? extends IndexNode<R, A>>, Optional<R>>() {
//                            @Override
//                            public Optional<R> apply(Map.Entry<Range<A>, ? extends IndexNode<R, A>> input) {
//                                if (range.encloses(input.getKey())) {
//                                    return Optional.of(input.getValue().aggregatedValue());
//                                }
//                                if (range.isConnected(input.getKey())) {
//                                    return Optional.of(evaluate(range, input.getValue()));
//                                }
//                                return Optional.empty();
//                            }
//                        }));
    }

    private void processRange(Range<A> range, List<R> iterable, Range<A> currentRange, IndexNode<R, A> child) {
        if (range.encloses(currentRange)) {
            iterable.add(child.aggregatedValue());
        } else if (range.isConnected(currentRange)) {
            iterable.add(evaluate(currentRange, child));
        } //else continue
    }

    public static <F, T> Iterable<T> transformAndFilter(final Iterable<F> fromIterable,
                                                        final Function<? super F, Optional<T>> function) {
        Iterable<Optional<? extends T>> transformed = Iterables.transform(fromIterable, function);
        Iterable<Optional<? extends T>> filtered = Iterables.filter(transformed, Optional::isPresent);
        return Iterables.transform(filtered, x -> x.get());
    }
}
