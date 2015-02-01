package ws.danasoft.eventstore.storage;

import org.junit.Assert;
import org.junit.Test;
import ws.danasoft.eventstore.data.DataObject;
import ws.danasoft.eventstore.data.ObjectStamp;

import java.util.Arrays;
import java.util.Date;

import static org.mockito.Mockito.mock;

public class StorageTest {
    static {
        System.setProperty("StorageNode.maxCapacity", "3");      //TODO fork required for testing then?
    }

    @Test
    public void appendTest() {
        final Storage storage = new Storage();

        ObjectStamp aStamp = new ObjectStamp(new Date(0), 0);
        DataObject a = mock(DataObject.class, "a");
        ObjectStamp bStamp = new ObjectStamp(new Date(1), 0);
        DataObject b = mock(DataObject.class, "b");
        ObjectStamp cStamp = new ObjectStamp(new Date(2), 0);
        DataObject c = mock(DataObject.class, "c");
        ObjectStamp dStamp = new ObjectStamp(new Date(3), 0);
        DataObject d = mock(DataObject.class, "d");
        storage.add(aStamp, a);
        storage.add(bStamp, b);
        storage.add(cStamp, c);
        storage.add(dStamp, d);

        Assert.assertEquals(Arrays.asList(
                        Arrays.asList(
                                new StorageNode.ChunkEntry(aStamp, a),
                                new StorageNode.ChunkEntry(bStamp, b),
                                new StorageNode.ChunkEntry(cStamp, c)),
                        Arrays.asList(
                                new StorageNode.ChunkEntry(dStamp, d))),
                storage.all());
    }

    @Test
    public void insertTest() {
        final Storage storage = new Storage();

        ObjectStamp aStamp = new ObjectStamp(new Date(0), 0);
        DataObject a = mock(DataObject.class, "a");
        ObjectStamp bStamp = new ObjectStamp(new Date(1), 0);
        DataObject b = mock(DataObject.class, "b");
        ObjectStamp cStamp = new ObjectStamp(new Date(2), 0);
        DataObject c = mock(DataObject.class, "c");
        ObjectStamp dStamp = new ObjectStamp(new Date(3), 0);
        DataObject d = mock(DataObject.class, "d");
        storage.add(aStamp, a);
        storage.add(bStamp, b);
        storage.add(dStamp, d);
        storage.add(cStamp, c);

        Assert.assertEquals(Arrays.asList(
                        Arrays.asList(
                                new StorageNode.ChunkEntry(aStamp, a),
                                new StorageNode.ChunkEntry(bStamp, b),
                                new StorageNode.ChunkEntry(cStamp, c)),
                        Arrays.asList(
                                new StorageNode.ChunkEntry(dStamp, d))),
                storage.all());
    }
}
