package ws.danasoft.eventstore.cmd;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Range;
import ws.danasoft.eventstore.index.*;
import ws.danasoft.eventstore.math.Sum;

import java.io.File;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class ReadSpeedTest {
    private static final int MAX_BOUNDARIES = Integer.getInteger("maxBoundaries", 10);

    public static void main(String[] args) throws IOException {
        if (args.length != 5) {
            System.err.println("Usage: ReadSpeedTest file sampleLength loops regionSize regionsCount");
            System.exit(1);
        }
        String fileName = args[0];
        long to = Long.parseLong(args[1]);
        long loops = Long.parseLong(args[2]);
        long regionSize = Long.parseLong(args[3]);
        long regionCount = Long.parseLong(args[4]);
        File file = new File(fileName);
        if (!file.exists()) {
            System.err.println("File does not exists");
            System.exit(2);
        }
        BTreeNodeConfiguration<Long, Long> configuration = new BTreeNodeConfiguration<>(MAX_BOUNDARIES,
                new AggregatorValueUpdater<>(new Sum<>()), new LongLongBTreeSerializer());
        RegionMapper regionMapper = new FseekRegionMapper(file.toPath(), configuration.elementSize());
        BTree<Long, Long> bTree = BTree.load(configuration, regionMapper);
        AggregationIndexReader<Long, Long> aggregationIndexReader = new AggregationIndexReader<>(bTree, new Sum<>());
        final Random random = new Random();
        System.out.println("Warming up with " + loops / 10 + " loops");
        for (long i = 0; i < loops / 10; i++) {
            long regionStart = (long) (random.nextDouble() * (to - regionSize * regionCount));
            for (long start = regionStart; start < regionStart + regionSize * regionCount; start += regionSize) {
                aggregationIndexReader.evaluate(Range.openClosed(start, start + regionSize));
            }
        }
        FseekMappedRegion.READS = 0;
        FseekMappedRegion.REGIONS_READ.clear();
        System.out.println("Running " + loops + " working loops");
        Stopwatch stopwatch = Stopwatch.createStarted();
        int nodesVisited = 0;
        int leafsVisited = 0;
        for (long i = 0; i < loops; i++) {
            long regionStart = (long) (random.nextDouble() * (to - regionSize * regionCount));
            for (long start = regionStart; start < regionStart + regionSize * regionCount; start += regionSize) {
                AggregationIndexReader<Long, Long>.EvaluationResult result =
                        aggregationIndexReader._evaluate(Range.openClosed(start, start + regionSize));
                nodesVisited += result.getNodesVisited();
                leafsVisited += result.getLeafsVisited();
            }
        }
        stopwatch.stop();
        System.out.printf("%d ms per range evaluation%n", stopwatch.elapsed(TimeUnit.MILLISECONDS) / loops);
        System.out.printf("%d ns per region evaluation%n", stopwatch.elapsed(TimeUnit.NANOSECONDS) / loops / regionCount);
        System.out.printf("Total: %s%n", stopwatch);
        System.out.printf("Visited nodes: %d(%d bytes), leafs: %d(%d bytes)%n",
                nodesVisited, nodesVisited * configuration.elementSize(),
                leafsVisited, leafsVisited * configuration.elementSize()
        );
        System.out.printf("IO reads performed: %d (%d regions covered)%n", FseekMappedRegion.READS, FseekMappedRegion.REGIONS_READ.size());
    }
}
