package ws.danasoft.eventstore.math;

public class Sum<T extends Number> implements Aggregator<Long, T> {
    @Override
    public Long aggregateValues(Iterable<T> a) {
        long result = 0;
        for (T x : a) {
            result += x.longValue();
        }
        return result;
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
