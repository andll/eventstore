package ws.danasoft.eventstore.index;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashSet;
import java.util.Set;

public class FseekMappedRegion implements MappedRegion {
    public final static Set<Long> REGIONS_READ = new HashSet<>();
    public static int READS = 0;
    private final FileChannel channel;
    private final long pos;
    private final ByteBuffer bb;
    private boolean read = false;

    public FseekMappedRegion(FileChannel channel, long pos, int size) {
        this.channel = channel;
        this.pos = pos;
        bb = ByteBuffer.allocate(size);
    }

    @Override
    public void position(int position) {
        init();
        bb.position(position);
    }

    @Override
    public long getLong() {
        init();
        return bb.getLong();
    }

    @Override
    public long getLong(int offset) {
        init();
        return bb.getLong(offset);
    }

    @Override
    public void putLong(long v) {
        init();
        bb.putLong(v);
    }

    @Override
    public void putLong(int offset, long v) {
        init();
        bb.putLong(offset, v);
    }

    @Override
    public void flush() {
        bb.position(0);
        try {
            channel.position(pos);
            channel.write(bb);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void init() {
        if (read) {
            return;
        }
        read = true;
        REGIONS_READ.add(pos / 4096);
        READS++;
        bb.position(0);
        try {
            channel.position(pos);
            channel.read(bb);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
