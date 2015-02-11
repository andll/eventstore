package ws.danasoft.eventstore.storage;

import org.junit.Assert;
import org.junit.Test;
import ws.danasoft.eventstore.index.BTree;
import ws.danasoft.eventstore.index.BTreeNode;
import ws.danasoft.eventstore.index.BTreeNodeConfiguration;
import ws.danasoft.eventstore.index.LongLongBTreeSerializer;

import java.io.File;
import java.io.IOException;
import java.util.Optional;

public abstract class AbstractBlockStorageTest {
    private static final int MAX_BOUNDARIES = 2;
    private static final BTreeNodeConfiguration<Long, Long> CONFIGURATION = new BTreeNodeConfiguration<>(MAX_BOUNDARIES, (x) -> 1l,
            new LongLongBTreeSerializer());
    private static final long COUNT = 100;

    @Test
    public void writeReadTest() throws IOException {
        File tempFile = File.createTempFile("unitTest-", ".btree");
        try {
            write(tempFile, COUNT);
            test(tempFile);
        } finally {
            tempFile.delete();
        }
    }

    @Test
    public void addTest() throws IOException {
        File tempFile = File.createTempFile("unitTest-", ".btree");
        try {
            write(tempFile, COUNT / 2);
            add(tempFile);
            test(tempFile);
        } finally {
            tempFile.delete();
        }
    }

    private void test(File tempFile) throws IOException {
        try (BlockStorage storage = openStorageForRead(tempFile)) {
            BTree<Long, Long> btree = BTree.load(CONFIGURATION, storage);
            for (long i = 0; i < COUNT; i++) {
                Optional<BTreeNode<Long, Long>> lookup = btree.lookup(i);
                Assert.assertTrue("Can not find element at " + i, lookup.isPresent());
                Assert.assertEquals(i, lookup.get().getValue().longValue());
            }
            Assert.assertFalse(btree.lookup(-1l).isPresent());
            Assert.assertFalse(btree.lookup(COUNT + 1l).isPresent());
        }
    }

    private void write(File tempFile, long count) throws IOException {
        try (BlockStorage storage = createStorage(tempFile)) {
            BTree<Long, Long> btree = BTree.createNew(CONFIGURATION, storage);
            for (long i = 0; i < count; i++) {
                btree.put(i, i);
            }
        }
    }

    private void add(File tempFile) throws IOException {
        try (BlockStorage storage = openStorageForWrite(tempFile)) {
            BTree<Long, Long> btree = BTree.load(CONFIGURATION, storage);
            for (long i = COUNT / 2; i < COUNT; i++) {
                btree.put(i, i);
            }
        }
    }

    protected abstract BlockStorage createStorage(File tempFile) throws IOException;

    protected abstract BlockStorage openStorageForRead(File tempFile) throws IOException;

    protected abstract BlockStorage openStorageForWrite(File tempFile) throws IOException;
}
