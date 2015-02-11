package ws.danasoft.eventstore.storage;

import ws.danasoft.eventstore.index.MappedRegion;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

public class MmapBlockStorage extends BlockStorage {
    private final FileChannel channel;
    private final FileChannel.MapMode mapMode;
    private final Map<Long, MappedByteBuffer> mappedBuffers = new HashMap<>();

    private MmapBlockStorage(FileChannel channel, FileChannel.MapMode mapMode) {
        this.channel = channel;
        this.mapMode = mapMode;
    }

    public static MmapBlockStorage open(Path path, FileOpenMode mode) throws IOException {
        MmapBlockStorage storage = new MmapBlockStorage(mode.createFileChannel(path), toMapMode(mode));
        storage.init(false);
        return storage;
    }

    public static MmapBlockStorage create(Path path) throws IOException {
        MmapBlockStorage storage = new MmapBlockStorage(FileOpenMode.READ_WRITE.createFileChannel(path), FileChannel.MapMode.READ_WRITE);
        storage.init(true);
        return storage;
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
                mappedBuffers.put(chunk, mappedBuffer = channel.map(mapMode, chunk * CHUNK_SIZE, CHUNK_SIZE));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return mappedBuffer;
    }

    @Override
    public void close() throws IOException {
        super.close();
        for (MappedByteBuffer mappedBuffer : mappedBuffers.values()) {
            mappedBuffer.force();
        }
        channel.close();
    }

    private static FileChannel.MapMode toMapMode(FileOpenMode mode) {
        switch (mode) {
            case READ_ONLY:
                return FileChannel.MapMode.READ_ONLY;
            case READ_WRITE:
                return FileChannel.MapMode.READ_WRITE;
            default:
                throw new IllegalArgumentException("Unknown file open mode: " + mode);
        }
    }
}
