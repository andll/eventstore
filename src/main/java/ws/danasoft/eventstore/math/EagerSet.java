package ws.danasoft.eventstore.math;

import com.google.common.collect.Sets;

import java.util.HashSet;
import java.util.Set;

public class EagerSet<T> implements Aggregator<Set<T>, T> {
    @Override
    public Set<T> aggregateValues(Iterable<T> a) {
        return Sets.newHashSet(a);
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
