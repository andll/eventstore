package ws.danasoft.eventstore.index;

import com.google.common.base.Preconditions;

import java.util.Collections;
import java.util.Optional;

public class BTree<K extends Comparable<K>, V> {
    private static final int HEADER_SIZE = 8;
    private static final int HEADER_POSITION = 0;
    private final BTreeNodeConfiguration<K, V> configuration;
    private final RegionMapper regionMapper;
    private final MappedRegion rootPositionHolderBuffer;
    private Optional<BTreeNode<K, V>> root = Optional.empty();

    private BTree(BTreeNodeConfiguration<K, V> configuration, RegionMapper regionMapper, MappedRegion rootPositionHolderBuffer) {
        this.configuration = configuration;
        this.regionMapper = regionMapper;
        this.rootPositionHolderBuffer = rootPositionHolderBuffer;
    }

    public static <K extends Comparable<K>, V> BTree<K, V> createNew(BTreeNodeConfiguration<K, V> configuration, RegionMapper regionMapper) {
        long rootRegion = regionMapper.allocateRegion(HEADER_SIZE);
        Preconditions.checkState(rootRegion == HEADER_POSITION, "regionMapper returned illegal root region");
        MappedRegion rootBuffer = regionMapper.mapRegion(rootRegion, HEADER_SIZE);
        return new BTree<>(configuration, regionMapper, rootBuffer);
    }

    public static <K extends Comparable<K>, V> BTree<K, V> load(BTreeNodeConfiguration<K, V> configuration, RegionMapper regionMapper) {
        MappedRegion rootPositionHolderBuffer = regionMapper.mapRegion(HEADER_POSITION, HEADER_SIZE);
        BTree<K, V> btree = new BTree<>(configuration, regionMapper, rootPositionHolderBuffer);
        long rootPosition = rootPositionHolderBuffer.getLong(0);
        if (rootPosition != LongLongBTreeSerializer.NO_VALUE) {
            btree.root = Optional.of(BTreeNode.map(configuration, regionMapper, rootPosition));
        }
        return btree;
    }

    public void put(K key, V value) {
        if (root.isPresent()) {
            updateRoot(root.get().add(key, value));
        } else {
            updateRoot(BTreeNode.allocateLeaf(configuration, regionMapper, key, value));
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
