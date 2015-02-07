package ws.danasoft.eventstore.index;

import com.google.common.base.Preconditions;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;

public class MmapRegionMapper extends RegionMapper {
    private final FileChannel channel;
    private final long maxChunkSize;
    private final Map<Long, MappedByteBuffer> mappedBuffers = new HashMap<>();

    public MmapRegionMapper(Path path, int regionSize) throws IOException {
        super(regionSize);
        channel = FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        maxChunkSize = Integer.MAX_VALUE / regionSize * regionSize;
    }

    @Override
    public MappedRegion mapRegion(long position) {
        Preconditions.checkArgument(position % regionSize == 0, "Position not aligned");
        long chunk = position / maxChunkSize;

        MappedByteBuffer mappedBuffer = mappedBuffers.get(chunk);
        if (mappedBuffer == null) {
            try {
                mappedBuffers.put(chunk, mappedBuffer = channel.map(FileChannel.MapMode.READ_WRITE, chunk * maxChunkSize, maxChunkSize));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        int positionInChunk = (int) (position - chunk * maxChunkSize);
        mappedBuffer.position(positionInChunk);
        ByteBuffer slice = mappedBuffer.slice();
        slice.limit(regionSize);
        return new ByteBufferRegion(slice);
    }

    @Override
    public void close() throws IOException {
        for (MappedByteBuffer mappedBuffer : mappedBuffers.values()) {
            mappedBuffer.force();
        }
        channel.close();
    }
}
