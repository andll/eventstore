package ws.danasoft.eventstore.index;

import com.google.common.base.Preconditions;
import ws.danasoft.eventstore.storage.BlockStorage;

import java.util.Collections;
import java.util.Optional;

public class BTree<K extends Comparable<K>, V> {
    private final BTreeNodeConfiguration<K, V> configuration;
    private final BlockStorage blockStorage;
    private final MappedRegion rootPositionHolderBuffer;
    private Optional<BTreeNode<K, V>> root = Optional.empty();

    private BTree(BTreeNodeConfiguration<K, V> configuration, BlockStorage blockStorage, MappedRegion rootPositionHolderBuffer) {
        this.configuration = configuration;
        this.blockStorage = blockStorage;
        this.rootPositionHolderBuffer = rootPositionHolderBuffer;
    }

    public static <K extends Comparable<K>, V> BTree<K, V> createNew(BTreeNodeConfiguration<K, V> configuration, BlockStorage blockStorage) {
        long rootRegion = blockStorage.allocateRegion(BlockStorage.LONG_SIZE);
        Preconditions.checkState(rootRegion == BlockStorage.FIRST_REGION_POSITION, "blockStorage returned illegal first region");
        MappedRegion rootBuffer = blockStorage.mapRegion(rootRegion, BlockStorage.LONG_SIZE);
        return new BTree<>(configuration, blockStorage, rootBuffer);
    }

    public static <K extends Comparable<K>, V> BTree<K, V> load(BTreeNodeConfiguration<K, V> configuration, BlockStorage blockStorage) {
        MappedRegion rootPositionHolderBuffer = blockStorage.mapRegion(BlockStorage.FIRST_REGION_POSITION, BlockStorage.LONG_SIZE);
        BTree<K, V> btree = new BTree<>(configuration, blockStorage, rootPositionHolderBuffer);
        long rootPosition = rootPositionHolderBuffer.getLong(0);
        if (rootPosition != LongLongBTreeSerializer.NO_VALUE) {
            btree.root = Optional.of(BTreeNode.map(configuration, blockStorage, rootPosition));
        }
        return btree;
    }

    public void put(K key, V value) {
        if (root.isPresent()) {
            updateRoot(root.get().add(key, value));
        } else {
            updateRoot(BTreeNode.allocateLeaf(configuration, blockStorage, key, value));
        }
    }

    private void updateRoot(BTreeNode<K, V> newRoot) {
        if (root.isPresent() && root.get() == newRoot) {
            return;
        }
        rootPositionHolderBuffer.putLong(0, newRoot.getPosition());
        rootPositionHolderBuffer.flush();
        root = Optional.of(newRoot);
    }

    public Optional<BTreeNode<K, V>> getRoot() {
        return root;
    }

    public boolean isEmpty() {
        return !root.isPresent();
    }

    public Optional<BTreeNode<K, V>> lookup(K key) {
        return lookup(root.get(), key);
    }

    private Optional<BTreeNode<K, V>> lookup(BTreeNode<K, V> node, K key) {
        if (node.isLeaf()) {
            if (node.getKey().equals(key)) {
                return Optional.of(node);
            }
            return Optional.empty();
        }
        int search = Collections.binarySearch(node.getBoundaries(), key);
        int position;
        if (search < 0) {
            position = -(search + 1);
        } else {
            position = search + 1;
        }

        return lookup(node.getNode(position), key);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        BTree bTree = (BTree) o;

        if (!configuration.equals(bTree.configuration)) return false;
        if (!root.equals(bTree.root)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = configuration.hashCode();
        result = 31 * result + root.hashCode();
        return result;
    }
}
