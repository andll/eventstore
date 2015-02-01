package ws.danasoft.eventstore.math;

public interface Aggregator<R, A> {
    R aggregateValues(Iterable<A> a);

    R aggregate(Iterable<R> a);
}
