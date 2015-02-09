package ws.danasoft.eventstore.index;

import com.google.common.base.Preconditions;

import java.io.Closeable;
import java.io.IOException;

public abstract class RegionMapper implements Closeable {
    protected static final long CHUNK_SIZE = Integer.MAX_VALUE; //All region aligned so they fit some chunk
    private long lastPosition = 0; //TODO update if open existing

    public abstract MappedRegion mapRegion(long position, int size);

    public abstract long getLong(long position);

    public final long allocateRegion(int size) {
        long position = lastPosition;
        if (position / CHUNK_SIZE != (position + size) / CHUNK_SIZE) {
            position = (position / CHUNK_SIZE + 1) * CHUNK_SIZE;
        }
        lastPosition = position + size;
        return position;
    }

    //This rules are same for all implementations of RegionMapper so files created with some mappers can be then accessed with others
    protected final void checkRegion(long position, int size) {
        Preconditions.checkArgument(size > 0, "Size should be positive");
        Preconditions.checkArgument(position >= 0, "Position should be non-negative");

        Preconditions.checkArgument(position / CHUNK_SIZE == (position + size) / CHUNK_SIZE,
                "Can not map region that crosses chunk boundaries");
    }

    @Override
    public abstract void close() throws IOException;
}
