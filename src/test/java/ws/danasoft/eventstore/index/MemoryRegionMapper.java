package ws.danasoft.eventstore.index;

import com.google.common.base.Preconditions;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class MemoryRegionMapper extends RegionMapper {
    private final Map<Long, ByteBuffer> data = new HashMap<>();

    public MemoryRegionMapper(int regionSize) {
        super(regionSize);
    }

    @Override
    public MappedRegion mapRegion(long position) {
        Preconditions.checkArgument(position % regionSize == 0, "Position not aligned");
        ByteBuffer buffer = data.get(position);
        if (buffer == null) {
            data.put(position, buffer = ByteBuffer.allocate(regionSize));
        }
        return new ByteBufferRegion(buffer);
    }

    @Override
    public void close() throws IOException {
    }
}
