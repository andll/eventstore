package ws.danasoft.eventstore.index;

public interface MappedRegion {
    void position(int position);

    long getLong();

    long getLong(int offset);

    void putLong(long v);

    void putLong(int offset, long v);

    void flush();
}
