package ws.danasoft.eventstore.storage;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public enum FileOpenMode {
    READ_ONLY, READ_WRITE;

    FileChannel createFileChannel(Path path) throws IOException {
        switch (this) {
            case READ_ONLY:
                return FileChannel.open(path, StandardOpenOption.READ);
            case READ_WRITE:
                return FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
            default:
                throw new IllegalArgumentException("Unknown file mode");
        }
    }
}
