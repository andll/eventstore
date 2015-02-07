package ws.danasoft.eventstore.index;

import com.google.common.collect.BoundType;
import com.google.common.collect.Collections2;
import com.google.common.collect.Range;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;

public class BTreeTest {
    private static final int MAX_BOUNDARIES = 2;
    private static final BTreeNodeConfiguration<Long, Long> CONFIGURATION = new BTreeNodeConfiguration<>(MAX_BOUNDARIES, (x) -> 1l,
            new LongLongBTreeSerializer());

    @Test
    public void twoElementsTest() {
        checkAllPermutations(Arrays.asList(1l, 3l));
    }

    @Test
    public void threeElementsTest() {
        checkAllPermutations(Arrays.asList(1l, 2l, 3l));
    }

    @Test
    public void fourElementsTest() {
        checkAllPermutations(Arrays.asList(1l, 2l, 3l, 4l));
    }

    @Test
    public void fiveElementsTest() {
        checkAllPermutations(Arrays.asList(1l, 2l, 3l, 4l, 5l));
    }

    @Test
    public void sixElementsTest() {
        checkAllPermutations(Arrays.asList(1l, 2l, 3l, 4l, 5l, 6l));
    }

    @Test
    public void fourElementsWithDuplicationsTest() {
        checkAllPermutations(Arrays.asList(1l, 2l, 3l, 4l, 1l, 2l, 3l, 4l));
    }

    @Test
    public void thousandElements_RandomPermutationsTest() {
        List<Long> data = new ArrayList<>();
        for (long i = 0; i < 1000; i++) {
            data.add(i);
        }
        for (int i = 0; i < 10; i++) {
            Collections.shuffle(data);
            checkPermutation(data, true);
        }
    }

    private void checkAllPermutations(List<Long> elements) {
        Collection<List<Long>> permutations = Collections2.orderedPermutations(elements);
        for (List<Long> permutation : permutations) {
            checkPermutation(permutation, false);
        }
    }

    private void checkPermutation(List<Long> permutation, boolean onlyFinal) {
        BTree<Long, Long> btree = BTreeTestHelper.empty(CONFIGURATION);
        Set<Long> expectedKeys = new LinkedHashSet<>();
        try {
            for (Long e : permutation) {
                btree.put(e, e + 1);
                expectedKeys.add(e);
                if (!onlyFinal) {
                    assertValidSearchTree(btree, expectedKeys);
                }
            }
            if (onlyFinal) {
                assertValidSearchTree(btree, expectedKeys);
            }
        } catch (Throwable th) {
            throw new AssertionError("Failed to evaluate permutation " + expectedKeys + " tree: " + btree, th);
        }
    }

    private static void assertValidSearchTree(BTree<Long, Long> btree, Set<Long> expectedKeys) {
        HashSet<Long> allKeys = new HashSet<>();
        assertValidSearchTree(btree.getRoot().get(), Range.<Long>all(), allKeys, 0, expectedKeys.size());
        Assert.assertEquals("Tree keys", allKeys, expectedKeys);
    }

    private static void assertValidSearchTree(BTreeNode<Long, Long> btree, Range<Long> keyRange, Set<Long> allKeys, int depth, int totalKeys) {
        if (!allKeys.isEmpty()) {
            double log = logMaxBoundaries(totalKeys);
            Assert.assertTrue(String.format("Tree has %d keys and %d depth, which is too deep, log is %s", totalKeys, depth, log),
                    depth <= log + 2);
        }
        if (btree.isLeaf()) {
            Long key = btree.getKey();
            if (!allKeys.add(key)) {
                Assert.fail("Duplicated key: " + key);
            }
            Assert.assertTrue(String.format("Search range %s does not contain key %d", keyRange, key),
                    keyRange.contains(key));
            Assert.assertEquals("Value at key " + key, key + 1, btree.getValue().longValue());
        } else {
            Long prevBoundary = null;
            List<Long> boundaries = btree.getBoundaries();
            List<BTreeNode<Long, Long>> nodes = btree.getNodes();
            for (int i = 0; i < boundaries.size(); i++) {
                Long boundary = boundaries.get(i);
                Range<Long> range;
                if (prevBoundary == null) {
                    range = Range.lessThan(boundary);
                } else {
                    range = Range.range(prevBoundary, BoundType.CLOSED, boundary, BoundType.OPEN);
                }
                assertValidSearchTree(nodes.get(i), range.intersection(keyRange), allKeys, depth + 1, totalKeys);
                prevBoundary = boundary;
            }
            assert prevBoundary != null;

            Range<Long> range = Range.atLeast(prevBoundary);
            assertValidSearchTree(nodes.get(nodes.size() - 1), range.intersection(keyRange), allKeys, depth + 1, totalKeys);
        }
    }

    private static double logMaxBoundaries(double x) {
        return Math.log(x) / Math.log(MAX_BOUNDARIES);
    }

    //
//    private static BTreeNode<Long, Long> leaf(long key, long value) {
//        return new BTreeNode<>(CONFIGURATION, regionMapper, buffer, Optional.of(key), value);
//    }
//
//    private static BTreeNode<Long, Long> tree(ArrayList<Long> boundaries, ArrayList<BTreeNode<Long, Long>> nodes) {
//        return new BTreeNode<>(CONFIGURATION, regionMapper, buffer, boundaries, nodes);
//    }
//
//    private static BTree<Long, Long> tree(BTreeNode<Long, Long> root) {
//        return new BTree<>(root);
//    }
//
//    private static BTree<Long, Long> twoNodesTree() {
//        return tree(tree(boundaries(3l), nodes(leaf(1, 2), leaf(3, 4))));
//    }
//
//    public static ArrayList<Long> boundaries(long... x) {
//        ArrayList<Long> r = new ArrayList<>(x.length);
//        for (long v : x) {
//            r.add(v);
//        }
//        return r;
//    }
//
//    public static ArrayList<BTreeNode<Long, Long>> nodes(BTreeNode<Long, Long>... x) {
//        ArrayList<BTreeNode<Long, Long>> r = new ArrayList<>(x.length);
//        for (BTreeNode<Long, Long> v : x) {
//            r.add(v);
//        }
//        return r;
//    }
}
