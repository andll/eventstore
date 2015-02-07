package ws.danasoft.eventstore.index;

import java.nio.ByteBuffer;

public class ByteBufferRegion implements MappedRegion {
    private final ByteBuffer bb;

    public ByteBufferRegion(ByteBuffer bb) {
        this.bb = bb;
    }

    @Override
    public void position(int position) {
        bb.position(position);
    }

    @Override
    public long getLong() {
        return bb.getLong();
    }

    @Override
    public long getLong(int offset) {
        return bb.getLong(offset);
    }

    @Override
    public void putLong(long v) {
        bb.putLong(v);
    }

    @Override
    public void putLong(int offset, long v) {
        bb.putLong(offset, v);
    }

    @Override
    public void flush() {
    }
}
