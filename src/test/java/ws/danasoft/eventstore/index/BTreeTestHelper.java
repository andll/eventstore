package ws.danasoft.eventstore.index;

class BTreeTestHelper {
    static BTree<Long, Long> empty(BTreeNodeConfiguration<Long, Long> configuration) {
        return BTree.createNew(configuration, new MemoryRegionMapper(configuration.elementSize()));
    }

    static BTree<Long, Long> generate(long from, long to, BTreeNodeConfiguration<Long, Long> configuration) {
        BTree<Long, Long> tree = empty(configuration);
        for (long l = from; l <= to; l++) {
            tree.put(l, l);
        }
        return tree;
    }
}
