package ws.danasoft.eventstore.storage;

import com.google.common.base.Preconditions;
import ws.danasoft.eventstore.index.MappedRegion;

import java.io.Closeable;
import java.io.IOException;
import java.util.Optional;

public abstract class BlockStorage implements Closeable {
    protected static final long CHUNK_SIZE = Integer.MAX_VALUE; //All region aligned so they fit some chunk
    public static final int LONG_SIZE = 8;
    public static final int FIRST_REGION_POSITION = LONG_SIZE;
    private static final int FILE_HEADER_POSITION = 0;
    private Tracking<Long> lastPosition;
    private Optional<MappedRegion> headerRegion;

    //TODO proper error handling?
    protected void init(boolean created) {
        MappedRegion headerRegion = mapRegion(FILE_HEADER_POSITION, LONG_SIZE);
        if (created) {
            lastPosition = Tracking.create((long) FIRST_REGION_POSITION);
        } else {
            lastPosition = Tracking.create(headerRegion.getLong(0));
        }
        this.headerRegion = Optional.of(headerRegion);
    }

    public abstract MappedRegion mapRegion(long position, int size);

    public abstract long getLong(long position);

    public final long allocateRegion(int size) {
        long position = lastPosition.getValue();
        if (position / CHUNK_SIZE != (position + size) / CHUNK_SIZE) {
            position = (position / CHUNK_SIZE + 1) * CHUNK_SIZE;
        }
        lastPosition.setValue(position + size);
        return position;
    }

    //This rules are same for all implementations of BlockStorage so files created with some storage implementations can be then accessed with others
    protected final void checkRegion(long position, int size) {
        Preconditions.checkArgument(size > 0, "Size should be positive");
        Preconditions.checkArgument(position >= 0, "Position should be non-negative");

        Preconditions.checkArgument(position / CHUNK_SIZE == (position + size) / CHUNK_SIZE,
                "Can not map region that crosses chunk boundaries");
    }

    @Override
    public void close() throws IOException {
        if (lastPosition.isChanged()) {
            MappedRegion headerRegion = this.headerRegion.get();
            headerRegion.putLong(0, lastPosition.getValue());
            headerRegion.flush();
        }
    }
}
