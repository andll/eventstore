package ws.danasoft.eventstore.index;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class FseekRegionMapper extends RegionMapper {
    private final FileChannel channel;

    public FseekRegionMapper(Path path, int regionSize) throws IOException {
        super(regionSize);
        channel = FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
    }

    @Override
    public MappedRegion mapRegion(long position) {
        Preconditions.checkArgument(position % regionSize == 0, "Position not aligned");
        return new FseekMappedRegion(channel, position, regionSize);
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }
}
