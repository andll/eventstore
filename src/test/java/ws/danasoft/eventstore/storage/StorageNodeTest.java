package ws.danasoft.eventstore.storage;

import org.junit.Assert;
import org.junit.Test;
import ws.danasoft.eventstore.data.DataObject;
import ws.danasoft.eventstore.data.ObjectStamp;

import java.util.Arrays;
import java.util.Date;

import static org.mockito.Mockito.mock;

public class StorageNodeTest {
    @Test
    public void testInsertions() {
        final StorageNode storageNode = new StorageNode();
        DataObject a = mock(DataObject.class, "a");
        DataObject b = mock(DataObject.class, "b");
        DataObject c = mock(DataObject.class, "c");
        ObjectStamp aStamp = new ObjectStamp(new Date(0), 0);
        ObjectStamp bStamp = new ObjectStamp(new Date(2), 0);
        ObjectStamp cStamp = new ObjectStamp(new Date(3), 0);
        storageNode.add(aStamp, a);
        storageNode.add(cStamp, c);
        storageNode.add(bStamp, b);

        Assert.assertEquals(Arrays.asList(
                        new StorageNode.ChunkEntry(aStamp, a),
                        new StorageNode.ChunkEntry(bStamp, b),
                        new StorageNode.ChunkEntry(cStamp, c)),
                storageNode.all());
    }

    @Test
    public void testOverwrite() {
        final StorageNode storageNode = new StorageNode();
        DataObject a = mock(DataObject.class, "a");
        DataObject b = mock(DataObject.class, "b");
        DataObject c = mock(DataObject.class, "c");
        ObjectStamp aStamp = new ObjectStamp(new Date(0), 0);
        ObjectStamp bStamp = new ObjectStamp(new Date(2), 0);
        storageNode.add(aStamp, a);
        storageNode.add(bStamp, b);
        storageNode.add(aStamp, c);

        Assert.assertEquals(Arrays.asList(
                        new StorageNode.ChunkEntry(aStamp, c),
                        new StorageNode.ChunkEntry(bStamp, b)),
                storageNode.all());
    }

    @Test
    public void testSplitInclusive() {
        final StorageNode storageNode = new StorageNode();
        DataObject a = mock(DataObject.class, "a");
        DataObject b = mock(DataObject.class, "b");
        DataObject c = mock(DataObject.class, "c");
        ObjectStamp aStamp = new ObjectStamp(new Date(0), 0);
        ObjectStamp bStamp = new ObjectStamp(new Date(2), 0);
        ObjectStamp cStamp = new ObjectStamp(new Date(3), 0);
        storageNode.add(aStamp, a);
        storageNode.add(bStamp, b);
        storageNode.add(cStamp, c);

        StorageNode split = storageNode.split(bStamp);

        Assert.assertEquals(Arrays.asList(
                        new StorageNode.ChunkEntry(aStamp, a)),
                storageNode.all());

        Assert.assertEquals(Arrays.asList(
                        new StorageNode.ChunkEntry(bStamp, b),
                        new StorageNode.ChunkEntry(cStamp, c)),
                split.all());
    }

    @Test
    public void testSplitExclusive() {
        final StorageNode storageNode = new StorageNode();
        DataObject a = mock(DataObject.class, "a");
        DataObject b = mock(DataObject.class, "b");
        DataObject c = mock(DataObject.class, "c");
        ObjectStamp aStamp = new ObjectStamp(new Date(0), 0);
        ObjectStamp bStamp = new ObjectStamp(new Date(2), 0);
        ObjectStamp cStamp = new ObjectStamp(new Date(3), 0);
        storageNode.add(aStamp, a);
        storageNode.add(bStamp, b);
        storageNode.add(cStamp, c);

        StorageNode split = storageNode.split(new ObjectStamp(new Date(1), 0));

        Assert.assertEquals(Arrays.asList(
                        new StorageNode.ChunkEntry(aStamp, a)),
                storageNode.all());

        Assert.assertEquals(Arrays.asList(
                        new StorageNode.ChunkEntry(bStamp, b),
                        new StorageNode.ChunkEntry(cStamp, c)),
                split.all());
    }
}
