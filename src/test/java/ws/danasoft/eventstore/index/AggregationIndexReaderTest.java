package ws.danasoft.eventstore.index;

import com.google.common.collect.BoundType;
import com.google.common.collect.Range;
import org.junit.Assert;
import org.junit.Test;
import ws.danasoft.eventstore.math.Aggregator;
import ws.danasoft.eventstore.math.Sum;

import java.io.IOException;

public class AggregationIndexReaderTest {
    private static final int MAX_BOUNDARIES = 2;
    private static final int MIN = 0;
    private static final int MAX = 27;
    private static final Aggregator<Long, Long> AGGREGATOR = new Sum<>();

    private static final BTreeNodeConfiguration<Long, Long> CONFIGURATION = new BTreeNodeConfiguration<>(MAX_BOUNDARIES,
            new AggregatorValueUpdater<>(AGGREGATOR), new LongLongBTreeSerializer());

    private final BTree<Long, Long> tree = BTreeTestHelper.generate(MIN, MAX, CONFIGURATION);
    private final AggregationIndexReader<Long, Long> reader = new AggregationIndexReader<>(tree, AGGREGATOR);

    @Test
    public void rangeWithSingleValue() throws IOException {
        Assert.assertEquals(Long.valueOf(3), reader.evaluate(Range.singleton(3l)));
    }

    @Test
    public void openOpenInterval() throws IOException {
        Assert.assertEquals(Long.valueOf(4), reader.evaluate(Range.range(3l, BoundType.OPEN, 5l, BoundType.OPEN)));
    }

    @Test
    public void openCloseInterval() throws IOException {
        Assert.assertEquals(Long.valueOf(9), reader.evaluate(Range.range(3l, BoundType.OPEN, 5l, BoundType.CLOSED)));
    }

    @Test
    public void closeOpenInterval() throws IOException {
        Assert.assertEquals(Long.valueOf(7), reader.evaluate(Range.range(3l, BoundType.CLOSED, 5l, BoundType.OPEN)));
    }

    @Test
    public void closeCloseInterval() throws IOException {
        Assert.assertEquals(Long.valueOf(12), reader.evaluate(Range.range(3l, BoundType.CLOSED, 5l, BoundType.CLOSED)));
    }

    @Test
    public void allIntervals() {
        for (BoundType lowerType : BoundType.values()) {
            for (BoundType upperType : BoundType.values()) {
                for (long i = MIN - 1; i < MAX + 1; i++) {
                    for (long ii = i; ii < MAX + 1; ii++) {
                        if (i == ii) {
                            if (lowerType != BoundType.CLOSED || upperType != BoundType.CLOSED) {
                                continue;
                            }
                        }
                        Range<Long> range = Range.range(i, lowerType, ii, upperType);
                        long correctValue = sum(range);
                        Assert.assertEquals(range.toString(), correctValue, reader.evaluate(range).longValue());
                    }
                }
            }
        }
    }

    private long sum(Range<Long> range) {
        long sum = 0;
        for (long i = MIN; i <= MAX; i++) {
            if (range.contains(i)) {
                sum += i;
            }
        }
        return sum;
    }
}
