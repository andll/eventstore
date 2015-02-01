package ws.danasoft.eventstore.math;

import com.google.common.collect.Iterables;

public class Count<T> implements Aggregator<Long, T> {
    @Override
    public Long aggregateValues(Iterable<T> a) {
        return (long) Iterables.size(a);
    }

    @Override
    public Long aggregate(Iterable<Long> a) {
        long result = 0;
        for (Long x : a) {
            result += x;
        }
        return result;
    }
}
