package ws.danasoft.eventstore.storage;

import ws.danasoft.eventstore.index.MappedRegion;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class MemoryBlockStorage extends BlockStorage {
    private final Map<Long, ByteBuffer> data = new HashMap<>();

    public MemoryBlockStorage() {
        init(true);
    }

    @Override
    public MappedRegion mapRegion(long position, int size) {
        checkRegion(position, size);
        ByteBuffer buffer = data.get(position);
        if (buffer == null) {
            data.put(position, buffer = ByteBuffer.allocate(size));
        } else {
            if (buffer.capacity() != size) {
                throw new IllegalArgumentException("Buffer was allocated with different size");
            }
        }
        return new ByteBufferRegion(buffer);
    }

    /**
     * Limited implementation, only works when position is offset 0 to region base
     */
    @Override
    public long getLong(long position) {
        return data.get(position).getLong(0);
    }

    @Override
    public void close() throws IOException {
        super.close();
    }
}
