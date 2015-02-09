package ws.danasoft.eventstore.cmd;

import ws.danasoft.eventstore.index.*;

import java.io.File;
import java.io.IOException;
import java.util.Optional;

public class ReadIntegralIndex {
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
                (x) -> 0l, new LongLongBTreeSerializer());
        RegionMapper regionMapper = new FseekRegionMapper(file.toPath());
        BTree<Long, Long> bTree = BTree.load(configuration, regionMapper);
        Optional<BTreeNode<Long, Long>> nodeFrom = bTree.lookup(from - 1);
        Optional<BTreeNode<Long, Long>> nodeTo = bTree.lookup(to);
        if (!nodeTo.isPresent()) {
            System.err.println("To node not found");
            System.exit(1);
        }
        System.out.println(nodeTo.get().getValue() - nodeFrom.map(BTreeNode::getValue).orElse(0l));
    }
}
