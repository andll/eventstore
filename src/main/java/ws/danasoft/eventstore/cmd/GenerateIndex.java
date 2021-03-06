package ws.danasoft.eventstore.cmd;

import ws.danasoft.eventstore.index.*;
import ws.danasoft.eventstore.math.Sum;
import ws.danasoft.eventstore.storage.BlockStorage;
import ws.danasoft.eventstore.storage.MmapBlockStorage;

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
        BTreeNodeConfiguration<Long, Long> configuration = new BTreeNodeConfiguration<>(MAX_BOUNDARIES,
                new AggregatorValueUpdater<>(new Sum<>()), new LongLongBTreeSerializer());
        BlockStorage blockStorage = MmapBlockStorage.create(file.toPath());
        BTree<Long, Long> bTree = BTree.createNew(configuration, blockStorage);
        for (long i = from; i <= to; i++) {
            bTree.put(i, i);
        }
        blockStorage.close();
    }
}
