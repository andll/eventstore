package ws.danasoft.eventstore.index;

import com.google.common.collect.BoundType;
import com.google.common.collect.Iterables;
import com.google.common.collect.Range;
import com.google.gson.stream.JsonWriter;
import org.junit.Assert;
import org.junit.Test;
import ws.danasoft.eventstore.math.Aggregator;
import ws.danasoft.eventstore.math.Sum;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;

public class AggregationIndexReaderTest {
    private final Aggregator<Long, Integer> aggregator = new Sum<>();

    @Test
    public void test1() throws IOException {
        TestIndexNodeImpl node = generate(0, 27);

        AggregationIndexReader<Long, Integer> reader = new AggregationIndexReader<>(node, aggregator);

//        try (JsonWriter jsonWriter = new JsonWriter(new OutputStreamWriter(System.out))) {
//            jsonWriter.setIndent(" ");
//            node.printTo(jsonWriter);
//        }

        Assert.assertEquals(Long.valueOf(12), reader.evaluate(Range.range(3, BoundType.OPEN, 5, BoundType.OPEN)));
        Assert.assertEquals(Long.valueOf(12), reader.evaluate(Range.range(3, BoundType.CLOSED, 5, BoundType.CLOSED)));
    }

    private TestIndexNodeImpl generate(int from, int to) {
        List<Integer> boundaries = new ArrayList<>();
        List<TestIndexNodeImpl> nodes = new ArrayList<>();
        int step = (to - from) / 3;
        if (step != 0) {
            for (int i = from; i < to - step; i += step) {
                nodes.add(generate(i, i + step));
                boundaries.add(i + step);
            }
            nodes.add(generate(to - step, to));
        }
        long aggregatedValue;
        if (nodes.isEmpty()) {
            aggregatedValue = from;
        } else {
            aggregatedValue = aggregator.aggregate(Iterables.transform(nodes, TestIndexNodeImpl::aggregatedValue));
        }
        return new TestIndexNodeImpl(boundaries, nodes, aggregatedValue);
    }
}
