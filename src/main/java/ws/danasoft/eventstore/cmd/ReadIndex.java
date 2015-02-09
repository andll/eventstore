package ws.danasoft.eventstore.cmd;

import com.google.common.collect.Range;
import ws.danasoft.eventstore.index.*;
import ws.danasoft.eventstore.math.Sum;

import java.io.File;
import java.io.IOException;

public class ReadIndex {
    private static final int MAX_BOUNDARIES = Integer.getInteger("maxBoundaries", 10);

    public static void main(String[] args) throws IOException {
        if (args.length != 3) {
            System.err.println("Usage: ReadIndex file from to - read from file");
            System.err.println("ReadIndex --test from to - evaluate result with processor");
            System.exit(1);
        }
        String fileName = args[0];
        long from = Long.parseLong(args[1]);
        long to = Long.parseLong(args[2]);
        if (fileName.equals("--test")) {
            long sum = 0;
            for (long i = from; i <= to; i++) {
                sum += i;
            }
            System.out.println("Test result:" + sum);
            return;
        }
        File file = new File(fileName);
        if (!file.exists()) {
            System.err.println("File does not exists");
            System.exit(2);
        }
        BTreeNodeConfiguration<Long, Long> configuration = new BTreeNodeConfiguration<>(MAX_BOUNDARIES,
                new AggregatorValueUpdater<>(new Sum<>()), new LongLongBTreeSerializer());
        RegionMapper regionMapper = new FseekRegionMapper(file.toPath());
        BTree<Long, Long> bTree = BTree.load(configuration, regionMapper);
        AggregationIndexReader<Long, Long> aggregationIndexReader = new AggregationIndexReader<>(bTree, new Sum<>());
        System.out.println(aggregationIndexReader.evaluate(Range.closed(from, to)));
    }
}
