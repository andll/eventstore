package ws.danasoft.eventstore.index;

import java.util.List;

public interface BTreeNode<K extends Comparable<K>> {
    List<K> boundaries();

    List<? extends BTreeNode<K>> nodes();

    static <K extends Comparable<K>> void validate(List<K> boundaries, List<? extends BTreeNode<K>> nodes) throws IllegalArgumentException {
        if (nodes.isEmpty() && boundaries.isEmpty()) {
            return;
        }
        if (nodes.size() != boundaries.size() + 1) {
            throw new IllegalArgumentException(String.format("Nodes size %d is illegal with boundaries size %d",
                    nodes.size(), boundaries.size()));
        }
        K prevBoundary = null;
        for (int i = 0; i < boundaries.size(); i++) {
            K boundary = boundaries.get(i);
            if (prevBoundary != null) {
                if (boundary.compareTo(prevBoundary) <= 0) {
                    throw new IllegalArgumentException(String.format("Boundary at %d %s is lower or equal then previous boundary %s",
                            i, boundary, prevBoundary));
                }
            }
            prevBoundary = boundary;
        }
    }
}
