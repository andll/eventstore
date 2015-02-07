package ws.danasoft.eventstore.index;

import com.google.common.collect.BoundType;
import com.google.common.collect.Range;
import ws.danasoft.eventstore.math.Aggregator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class AggregationIndexReader<R, A extends Comparable<A>> {
    private final BTree<A, R> tree;
    private final Aggregator<R, A> aggregator;

    public AggregationIndexReader(BTree<A, R> tree, Aggregator<R, A> aggregator) {
        this.tree = tree;
        this.aggregator = aggregator;
    }

    public R evaluate(Range<A> range) {
        EvaluationResult result = _evaluate(range);
        return result.result;
    }

    public EvaluationResult _evaluate(Range<A> range) {
        if (tree.isEmpty()) {
            return new EvaluationResult(aggregator.aggregateValues(Collections.<A>emptySet()), 0, 0);
        }
        return new Evaluation(range).evaluate(tree.getRoot().get(), Range.all());
    }

    public class EvaluationResult {
        private final R result;
        private final int nodesVisited;
        private final int leafsVisited;

        public EvaluationResult(R result, int nodesVisited, int leafsVisited) {
            this.result = result;
            this.nodesVisited = nodesVisited;
            this.leafsVisited = leafsVisited;
        }

        public R getResult() {
            return result;
        }

        public int getNodesVisited() {
            return nodesVisited;
        }

        public int getLeafsVisited() {
            return leafsVisited;
        }
    }

    private class Evaluation {
        private final Range<A> range;
        private int nodesVisited = 0;
        private int leafsVisited = 0;

        private Evaluation(Range<A> range) {
            this.range = range;
        }

        private EvaluationResult evaluate(BTreeNode<A, R> node, Range<A> overallRange) {
            R r = _evaluate(node, overallRange);

            return new EvaluationResult(r, nodesVisited, leafsVisited);
        }

        private R _evaluate(BTreeNode<A, R> node, Range<A> overallRange) {
            if (node.isLeaf()) {
                leafsVisited++;
                if (range.contains(node.getKey())) {
                    return aggregator.aggregate(Collections.singleton(node.getValue()));
                }
                return aggregator.aggregate(Collections.<R>emptySet());
            }
            List<BTreeNode<A, R>> nodes = node.getNodes();
            List<R> iterable = new ArrayList<>(nodes.size());
            nodesVisited++;
            List<A> boundaries = node.getBoundaries();
            A prevBoundary = null;
            for (int i = 0; i < boundaries.size(); i++) {
                A boundary = boundaries.get(i);
                Range<A> currentRange;
                if (prevBoundary == null) {
                    currentRange = Range.lessThan(boundary);
                } else {
                    currentRange = Range.range(prevBoundary, BoundType.CLOSED, boundary, BoundType.OPEN);
                }
                BTreeNode<A, R> child = nodes.get(i);
                Range<A> intersection = currentRange.intersection(overallRange);
                processRange(iterable, intersection, child);
                prevBoundary = boundary;
            }
            assert prevBoundary != null;
            Range<A> currentRange = Range.atLeast(prevBoundary);
            BTreeNode<A, R> child = nodes.get(boundaries.size());
            processRange(iterable, currentRange.intersection(overallRange), child);

            return aggregator.aggregate(iterable);
        }

        private void processRange(List<R> iterable, Range<A> currentRange, BTreeNode<A, R> child) {
            if (range.encloses(currentRange)) {
                nodesVisited++;
                iterable.add(child.getValue());
            } else if (range.isConnected(currentRange)) {
                iterable.add(_evaluate(child, currentRange));
            } //else continue
        }
    }
}
