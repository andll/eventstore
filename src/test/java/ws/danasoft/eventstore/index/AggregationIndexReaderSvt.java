package ws.danasoft.eventstore.index;

import com.google.common.base.Stopwatch;
import com.google.common.collect.BoundType;
import com.google.common.collect.Range;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import ws.danasoft.eventstore.math.Sum;

import java.util.function.Supplier;

public class AggregationIndexReaderSvt {
    private static final int MAX_BOUNDARIES = 10;
    private static final BTreeNodeConfiguration<Long, Long> CONFIGURATION = new BTreeNodeConfiguration<>(MAX_BOUNDARIES, (x) -> {
        long sum = 0;

        for (BTreeNode<Long, Long> node : x.getNodes()) {
            sum += node.getValue();
        }

        return sum;
    }, new LongLongBTreeSerializer());

    private static final long MIN = 0;
    private static final long MAX = 10 * 1000 * 1000;

    @Before
    public void before() {
        Assume.assumeTrue(Boolean.getBoolean("svt"));
    }

    @Test
    public void indexReferenceComparison() {
        final BTree<Long, Long> node = measure("generation", () -> BTreeTestHelper.generate(MIN, MAX, CONFIGURATION));
        long fromIncluding = 100 * 10 * 1000;
        long toExcluding = 810 * 10 * 1000;
        Long reference = measure("reference calculation", () -> sum(fromIncluding, toExcluding));
        AggregationIndexReader<Long, Long> reader = new AggregationIndexReader<>(node, new Sum<>());
        Range<Long> range = Range.range(fromIncluding, BoundType.CLOSED, toExcluding, BoundType.OPEN);
        AggregationIndexReader<Long, Long>.EvaluationResult evaluationResult =
                measure("index calculation", () -> reader._evaluate(range));

        System.out.printf("Nodes visited: %d%n", evaluationResult.getNodesVisited());

        Assert.assertEquals(reference, evaluationResult.getResult());
    }

    private long sum(long fromIncluding, long toExcluding) {
        long sum = 0;
        for (long i = fromIncluding; i < toExcluding; i++) {
            sum += i;
        }
        return sum;
    }

    private static <T> T measure(String name, Supplier<T> supplier) {
        Stopwatch stopwatch = Stopwatch.createStarted();
        T t = supplier.get();
        stopwatch.stop();
        System.out.printf("%s: %s%n", name, stopwatch);
        return t;
    }
}
