package ws.danasoft.eventstore.index;

import com.google.common.collect.BoundType;
import com.google.common.collect.Collections2;
import com.google.common.collect.Range;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;

public class BTreeNodeTest {
    private static final int MAX_BOUNDARIES = 2;

    @Test
    public void insertIntoEmptyTree_returnsLeaf() {
        BTreeNode<Long, Long> add = empty().add(1l, 2l);
        Assert.assertEquals(leaf(1l, 2l), add);
    }

    @Test
    public void insertIntoLeafSameKey_returnsNewLeaf() {
        BTreeNode<Long, Long> add = leaf(1, 2).add(1l, 3l);
        Assert.assertEquals(leaf(1l, 3l), add);
    }

    @Test
    public void insertIntoLeafNewLowerKey_returnsNewTree() {
        BTreeNode<Long, Long> add = leaf(1, 2).add(3l, 4l);
        Assert.assertEquals(twoNodesTree(), add);
    }

    @Test
    public void insertIntoLeafNewGreaterKey_returnsNewTree() {
        BTreeNode<Long, Long> add = leaf(3, 4).add(1l, 2l);
        Assert.assertEquals(twoNodesTree(), add);
    }

    @Test
    public void insertLowerExistingKeyToTree_modifiesTree() {
        BTreeNode<Long, Long> old = twoNodesTree();
        BTreeNode<Long, Long> add = old.add(1l, 10l);
        Assert.assertEquals(tree(boundaries(3l), nodes(leaf(1, 10), leaf(3, 4))), add);
    }

    @Test
    public void insertUpperExistingKeyToTree_modifiesTree() {
        BTreeNode<Long, Long> old = twoNodesTree();
        BTreeNode<Long, Long> add = old.add(3l, 10l);
        Assert.assertEquals(tree(boundaries(3l), nodes(leaf(1, 2), leaf(3, 10))), add);
    }

    @Test
    public void insertOneMoreKeyLowerThenAll_modifiesTree() {
        BTreeNode<Long, Long> old = twoNodesTree();
        BTreeNode<Long, Long> add = old.add(0l, 1l);
        Assert.assertSame(old, add);
        Assert.assertEquals(tree(boundaries(1l, 3l), nodes(leaf(0, 1), leaf(1, 2), leaf(3, 4))), add);
    }

    @Test
    public void insertOneMoreKeyBetweenExisting_modifiesTree() {
        BTreeNode<Long, Long> old = twoNodesTree();
        BTreeNode<Long, Long> add = old.add(2l, 3l);
        Assert.assertSame(old, add);
        Assert.assertEquals(tree(boundaries(2l, 3l), nodes(leaf(1, 2), leaf(2, 3), leaf(3, 4))), add);
    }

    @Test
    public void insertOneMoreKeyGreaterThenAll_modifiesTree() {
        BTreeNode<Long, Long> old = twoNodesTree();
        BTreeNode<Long, Long> add = old.add(4l, 5l);
        Assert.assertSame(old, add);
        Assert.assertEquals(tree(boundaries(3l, 4l), nodes(leaf(1, 2), leaf(3, 4), leaf(4, 5))), add);
    }

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
        BTreeNode<Long, Long> root = empty();
        Set<Long> expectedKeys = new LinkedHashSet<>();
        try {
            for (Long e : permutation) {
                root = root.add(e, e + 1);
                expectedKeys.add(e);
                if (!onlyFinal) {
                    assertValidSearchTree(root, expectedKeys);
                }
            }
            if (onlyFinal) {
                assertValidSearchTree(root, expectedKeys);
            }
        } catch (Throwable th) {
            throw new AssertionError("Failed to evaluate permutation " + expectedKeys + " tree: " + root, th);
        }
    }

    private static void assertValidSearchTree(BTreeNode<Long, Long> btree, Set<Long> expectedKeys) {
        HashSet<Long> allKeys = new HashSet<>();
        assertValidSearchTree(btree, Range.<Long>all(), allKeys, 0, expectedKeys.size());
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
            Assert.assertEquals("Value at key " + key, btree.getValue().longValue(), key + 1);
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

    private static BTreeNode<Long, Long> empty() {
        return BTreeNode.emptyTree(MAX_BOUNDARIES);
    }

    private static BTreeNode<Long, Long> leaf(long key, long value) {
        return new BTreeNode<>(MAX_BOUNDARIES, Optional.of(key), Optional.of(value));
    }

    private static BTreeNode<Long, Long> tree(ArrayList<Long> boundaries, ArrayList<BTreeNode<Long, Long>> nodes) {
        return new BTreeNode<>(MAX_BOUNDARIES, boundaries, nodes);
    }

    private static BTreeNode<Long, Long> twoNodesTree() {
        return tree(boundaries(3l), nodes(leaf(1, 2), leaf(3, 4)));
    }

    public static ArrayList<Long> boundaries(long... x) {
        ArrayList<Long> r = new ArrayList<>(x.length);
        for (long v : x) {
            r.add(v);
        }
        return r;
    }

    public static ArrayList<BTreeNode<Long, Long>> nodes(BTreeNode<Long, Long>... x) {
        ArrayList<BTreeNode<Long, Long>> r = new ArrayList<>(x.length);
        for (BTreeNode<Long, Long> v : x) {
            r.add(v);
        }
        return r;
    }
}
