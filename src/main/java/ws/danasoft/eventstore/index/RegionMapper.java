package ws.danasoft.eventstore.index;

import java.io.Closeable;
import java.io.IOException;

public abstract class RegionMapper implements Closeable {
    protected final int regionSize;
    private long lastPosition = 0; //TODO update if open existing

    public RegionMapper(int regionSize) {
        this.regionSize = regionSize;
    }

    public abstract MappedRegion mapRegion(long position);

    public long allocateRegion() {
        long position = lastPosition;
        lastPosition += regionSize;
        return position;
    }

    @Override
    public abstract void close() throws IOException;
}
