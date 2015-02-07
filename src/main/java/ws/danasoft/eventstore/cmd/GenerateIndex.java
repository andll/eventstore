package ws.danasoft.eventstore.cmd;

import ws.danasoft.eventstore.index.*;

import java.io.File;
import java.io.IOException;

public class GenerateIndex {
    private static final int MAX_BOUNDARIES = Integer.getInteger("maxBoundaries", 10);

    public static void main(String[] args) throws IOException {
        if (args.length != 3) {
            System.err.println("Usage: GenerateIndex file from to");
            System.exit(1);
        }
        String fileName = args[0];
        File file = new File(fileName);
        if (file.exists()) {
            file.delete();
        }
        long from = Long.parseLong(args[1]);
        long to = Long.parseLong(args[2]);
        BTreeNodeConfiguration<Long, Long> configuration = new BTreeNodeConfiguration<>(MAX_BOUNDARIES, (x) -> {
            long sum = 0;

            for (BTreeNode<Long, Long> node : x.getNodes()) {
                sum += node.getValue();
            }

            return sum;
        }, new LongLongBTreeSerializer());
        RegionMapper regionMapper = new MmapRegionMapper(file.toPath(), configuration.elementSize());
        BTree<Long, Long> bTree = BTree.createNew(configuration, regionMapper);
        for (long i = from; i <= to; i++) {
            bTree.put(i, i);
        }
        regionMapper.close();
    }
}
