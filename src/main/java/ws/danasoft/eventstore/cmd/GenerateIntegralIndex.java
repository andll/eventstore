package ws.danasoft.eventstore.cmd;

import ws.danasoft.eventstore.index.*;

import java.io.File;
import java.io.IOException;
import java.util.Random;

public class GenerateIntegralIndex {
    private static final int MAX_BOUNDARIES = Integer.getInteger("maxBoundaries", 10);

    public static void main(String[] args) throws IOException {
        if (args.length != 4) {
            System.err.println("Usage: GenerateIntegralIndex file from to rand|inc");
            System.exit(1);
        }
        String fileName = args[0];
        File file = new File(fileName);
        if (file.exists()) {
            file.delete();
        }
        long from = Long.parseLong(args[1]);
        long to = Long.parseLong(args[2]);
        Mode mode = Mode.valueOf(args[3].toUpperCase());
        BTreeNodeConfiguration<Long, Long> configuration = new BTreeNodeConfiguration<>(MAX_BOUNDARIES,
                (x) -> 0l, new LongLongBTreeSerializer());
        RegionMapper regionMapper = new MmapRegionMapper(file.toPath());
        BTree<Long, Long> bTree = BTree.createNew(configuration, regionMapper);
        generateTo(from, to, bTree, mode);
        regionMapper.close();
    }

    public static void generateTo(long from, long to, BTree<Long, Long> bTree, Mode mode) {
        long sum = 0;
        final Random random = new Random();
        int prevReportedPercent = -1;
        for (long i = from; i <= to; i++) {
            int percent = (int) (i * 10000 / (to - from));
            if (prevReportedPercent != percent) {
                System.out.print("\r" + percent / 100. + "%");
                prevReportedPercent = percent;
            }
            long p;
            switch (mode) {
                case RAND:
                    p = (long) (f(i * 7. / to) * (100 + random.nextGaussian() * 50));
                    break;
                case INC:
                    p = i;
                    break;
                default:
                    throw new IllegalArgumentException("Unknown mode: " + mode);
            }
            sum += p;
            bTree.put(i, sum);
        }
        System.out.println();
    }

    private static double f(double x) {
        return (Math.pow(x - 4, 3) + 3 * Math.pow(x - 4, 2) - 6 * (x - 4) - 8) / 4;
    }

    public enum Mode {
        RAND, INC
    }
}
