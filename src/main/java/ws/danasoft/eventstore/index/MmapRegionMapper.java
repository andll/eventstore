package ws.danasoft.eventstore.index;

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
    private final Map<Long, MappedByteBuffer> mappedBuffers = new HashMap<>();

    public MmapRegionMapper(Path path) throws IOException {
        channel = FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
    }

    @Override
    public MappedRegion mapRegion(long position, int size) {
        checkRegion(position, size);

        long chunk = position / CHUNK_SIZE;

        MappedByteBuffer mappedBuffer = mappedBufferForChunk(chunk);
        int positionInChunk = (int) (position - chunk * CHUNK_SIZE);
        mappedBuffer.position(positionInChunk);
        ByteBuffer slice = mappedBuffer.slice();
        slice.limit(size);
        return new ByteBufferRegion(slice);
    }

    @Override
    public long getLong(long position) {
        long chunk = position / CHUNK_SIZE;
        MappedByteBuffer mappedBuffer = mappedBufferForChunk(chunk);
        int positionInChunk = (int) (position - chunk * CHUNK_SIZE);
        return mappedBuffer.getLong(positionInChunk);
    }

    private MappedByteBuffer mappedBufferForChunk(long chunk) {
        MappedByteBuffer mappedBuffer = mappedBuffers.get(chunk);
        if (mappedBuffer == null) {
            try {
                mappedBuffers.put(chunk, mappedBuffer = channel.map(FileChannel.MapMode.READ_WRITE, chunk * CHUNK_SIZE, CHUNK_SIZE));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return mappedBuffer;
    }

    @Override
    public void close() throws IOException {
        for (MappedByteBuffer mappedBuffer : mappedBuffers.values()) {
            mappedBuffer.force();
        }
        channel.close();
    }
}
