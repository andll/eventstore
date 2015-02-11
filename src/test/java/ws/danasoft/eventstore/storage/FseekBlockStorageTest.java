package ws.danasoft.eventstore.storage;

import java.io.File;
import java.io.IOException;

public class FseekBlockStorageTest extends AbstractBlockStorageTest {
    @Override
    protected FseekBlockStorage createStorage(File tempFile) throws IOException {
        return FseekBlockStorage.create(tempFile.toPath());
    }

    @Override
    protected FseekBlockStorage openStorageForRead(File tempFile) throws IOException {
        return FseekBlockStorage.open(tempFile.toPath(), FileOpenMode.READ_ONLY);
    }

    @Override
    protected FseekBlockStorage openStorageForWrite(File tempFile) throws IOException {
        return FseekBlockStorage.open(tempFile.toPath(), FileOpenMode.READ_WRITE);
    }
}
