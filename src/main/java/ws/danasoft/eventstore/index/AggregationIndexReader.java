package ws.danasoft.eventstore.index;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.BoundType;
import com.google.common.collect.Range;
import ws.danasoft.eventstore.math.Aggregator;

import java.util.ArrayList;
import java.util.List;

public class AggregationIndexReader<R, A extends Comparable<A>> {
    private final BTreeNode<A, R> root;
    private final Aggregator<R, A> aggregator;

    public AggregationIndexReader(BTreeNode<A, R> root, Aggregator<R, A> aggregator) {
        this.root = root;
        this.aggregator = aggregator;
    }

    public R evaluate(Range<A> range) {
        EvaluationResult result = _evaluate(range);
        return result.result;
    }

    @VisibleForTesting
    EvaluationResult _evaluate(Range<A> range) {
        return new Evaluation(range).evaluate(root, Range.all());
    }

    @VisibleForTesting
    class EvaluationResult {
        private final R result;
        private final int nodesVisited;

        public EvaluationResult(R result, int nodesVisited) {
            this.result = result;
            this.nodesVisited = nodesVisited;
        }

        public R getResult() {
            return result;
        }

        public int getNodesVisited() {
            return nodesVisited;
        }
    }

    private class Evaluation {
        private final Range<A> range;
        private int nodesVisited = 0;

        private Evaluation(Range<A> range) {
            this.range = range;
        }

        private EvaluationResult evaluate(BTreeNode<A, R> node, Range<A> overallRange) {
            R r = _evaluate(node, overallRange);

            return new EvaluationResult(r, nodesVisited);
        }

        private R _evaluate(BTreeNode<A, R> node, Range<A> overallRange) {
            nodesVisited++;
            List<BTreeNode<A, R>> nodes = node.getNodes();
            List<R> iterable = new ArrayList<>(nodes.size());
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
            if (prevBoundary == null) {
                if (node.isLeaf()) { //can be empty tree
                    if (range.contains(node.getKey())) {
                        iterable.add(node.getValue());
                    }
                }
            } else {
                Range<A> currentRange = Range.atLeast(prevBoundary);
                BTreeNode<A, R> child = nodes.get(boundaries.size());
                processRange(iterable, currentRange.intersection(overallRange), child);
            }

            return aggregator.aggregate(iterable);
        }

        private void processRange(List<R> iterable, Range<A> currentRange, BTreeNode<A, R> child) {
            if (range.encloses(currentRange)) {
                iterable.add(child.getValue());
            } else if (range.isConnected(currentRange)) {
                iterable.add(_evaluate(child, currentRange));
            } //else continue
        }
    }
}
