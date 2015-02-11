package ws.danasoft.eventstore.storage;

import java.io.File;
import java.io.IOException;

public class MmapBlockStorageTest extends AbstractBlockStorageTest {
    @Override
    protected MmapBlockStorage createStorage(File tempFile) throws IOException {
        return MmapBlockStorage.create(tempFile.toPath());
    }

    @Override
    protected MmapBlockStorage openStorageForRead(File tempFile) throws IOException {
        return MmapBlockStorage.open(tempFile.toPath(), FileOpenMode.READ_ONLY);
    }

    @Override
    protected MmapBlockStorage openStorageForWrite(File tempFile) throws IOException {
        return MmapBlockStorage.open(tempFile.toPath(), FileOpenMode.READ_WRITE);
    }
}
