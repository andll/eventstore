package ws.danasoft.eventstore.index;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class FseekRegionMapper extends RegionMapper {
    private final FileChannel channel;

    public FseekRegionMapper(Path path) throws IOException {
        channel = FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
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
        channel.close();
    }
}
