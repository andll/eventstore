package ws.danasoft.eventstore.index;

import com.google.common.base.Preconditions;

import java.util.Optional;

public class BTree<K extends Comparable<K>, V> {
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
        long rootRegion = regionMapper.allocateRegion();
        Preconditions.checkState(rootRegion == 0, "regionMapper returned illegal root region");
        MappedRegion rootBuffer = regionMapper.mapRegion(rootRegion);
        return new BTree<>(configuration, regionMapper, rootBuffer);
    }

    public static <K extends Comparable<K>, V> BTree<K, V> load(BTreeNodeConfiguration<K, V> configuration, RegionMapper regionMapper) {
        MappedRegion rootPositionHolderBuffer = regionMapper.mapRegion(0);
        BTree<K, V> btree = new BTree<>(configuration, regionMapper, rootPositionHolderBuffer);
        long rootPosition = rootPositionHolderBuffer.getLong(0);
        if (rootPosition != LongLongBTreeSerializer.NO_VALUE) {
            btree.root = Optional.of(new BTreeNode<>(configuration, regionMapper, regionMapper.mapRegion(rootPosition), rootPosition));
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