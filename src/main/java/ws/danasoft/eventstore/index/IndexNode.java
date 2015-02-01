package ws.danasoft.eventstore.index;

import java.util.List;

public interface IndexNode<R, A extends Comparable<A>> extends BTreeNode<A> {
    @Override
    List<? extends IndexNode<R, A>> nodes();

    R aggregatedValue();
}
