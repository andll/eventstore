package ws.danasoft.eventstore.storage;

import ws.danasoft.eventstore.index.MappedRegion;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;

public class FseekBlockStorage extends BlockStorage {
    private final FileChannel channel;

    private FseekBlockStorage(FileChannel channel) {
        this.channel = channel;
    }

    public static FseekBlockStorage open(Path path, FileOpenMode mode) throws IOException {
        FseekBlockStorage storage = new FseekBlockStorage(mode.createFileChannel(path));
        storage.init(false);
        return storage;
    }

    public static FseekBlockStorage create(Path path) throws IOException {
        FseekBlockStorage storage = new FseekBlockStorage(FileOpenMode.READ_WRITE.createFileChannel(path));
        storage.init(true);
        return storage;
    }

    @Override
    public MappedRegion mapRegion(long position, int size) {
        checkRegion(position, size);

        return new FseekMappedRegion(channel, position, size);
    }

    @Override
    public long getLong(long position) {
        ByteBuffer buffer = ByteBuffer.allocate(8);
        try {
            channel.position(position);
            channel.read(buffer);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return buffer.getLong(0);
    }

    @Override
    public void close() throws IOException {
        super.close();
        channel.close();
    }
}
