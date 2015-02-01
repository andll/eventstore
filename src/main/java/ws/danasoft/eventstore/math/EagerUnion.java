package ws.danasoft.eventstore.math;

import java.util.HashSet;
import java.util.Set;

public class EagerUnion<T> implements Aggregator<Set<T>, Set<T>> {
    @Override
    public Set<T> aggregateValues(Iterable<Set<T>> a) {
        return aggregate(a);
    }

    @Override
    public Set<T> aggregate(Iterable<Set<T>> a) {
        HashSet<T> result = new HashSet<>();
        for (Set<T> x : a) {
            result.addAll(x);
        }
        return result;
    }
}
