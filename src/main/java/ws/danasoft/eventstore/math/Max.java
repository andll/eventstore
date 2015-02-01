package ws.danasoft.eventstore.math;

import java.util.Optional;

public class Max<T extends Comparable<T>> implements Aggregator<Optional<T>, T> {
    @Override
    public Optional<T> aggregateValues(Iterable<T> a) {
        T result = null;
        for (T x : a) {
            if (result == null) {
                result = x;
            } else {
                if (result.compareTo(x) < 0) {
                    result = x;
                }
            }
        }
        return Optional.ofNullable(result);
    }

    @Override
    public Optional<T> aggregate(Iterable<Optional<T>> a) {
        T result = null;
        for (Optional<T> ox : a) {
            if (!ox.isPresent()) {
                continue;
            }
            T x = ox.get();
            if (result == null) {
                result = x;
            } else {
                if (result.compareTo(x) < 0) {
                    result = x;
                }
            }
        }
        return Optional.ofNullable(result);
    }
}
