package ws.danasoft.eventstore.cmd;

import ws.danasoft.eventstore.index.*;

import java.io.File;
import java.io.IOException;
import java.util.Random;

public class GenerateIntegralIndex {
    private static final int MAX_BOUNDARIES = Integer.getInteger("maxBoundaries", 10);

    public static void main(String[] args) throws IOException {
        if (args.length != 3) {
            System.err.println("Usage: GenerateIntegralIndex file from to");
            System.exit(1);
        }
        String fileName = args[0];
        File file = new File(fileName);
        if (file.exists()) {
            file.delete();
        }
        long from = Long.parseLong(args[1]);
        long to = Long.parseLong(args[2]);
        BTreeNodeConfiguration<Long, Long> configuration = new BTreeNodeConfiguration<>(MAX_BOUNDARIES,
                (x) -> 0l, new LongLongBTreeSerializer());
        RegionMapper regionMapper = new MmapRegionMapper(file.toPath(), configuration.elementSize());
        BTree<Long, Long> bTree = BTree.createNew(configuration, regionMapper);
        long sum = 0;
        final Random random = new Random();
        for (long i = from; i <= to; i++) {
            long p = (long) (f(i * 7. / to) * (100 + random.nextGaussian() * 10));
            sum += p;
            bTree.put(i, sum);
        }
        regionMapper.close();
    }

    private static double f(double x) {
        return (Math.pow(x - 4, 3) + 3 * Math.pow(x - 4, 2) - 6 * (x - 4) - 8) / 4;
    }
}
