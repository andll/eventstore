package ws.danasoft.eventstore;

import ws.danasoft.eventstore.data.DataObject;
import ws.danasoft.eventstore.data.ObjectStamp;
import ws.danasoft.eventstore.storage.Storage;

import java.util.NoSuchElementException;

public class EventStore {
    private final Storage storage = new Storage();

    //Overwrites existing entry
    public void put(ObjectStamp stamp, DataObject object) {
        storage.add(stamp, object);
    }

    public void patch(ObjectStamp stamp, DataObject object) throws NoSuchElementException {
        throw new UnsupportedOperationException("patch");
    }

    public void delete(ObjectStamp stamp) throws NoSuchElementException {
        throw new UnsupportedOperationException("delete");
    }

    public DataObject get(ObjectStamp stamp) throws NoSuchElementException {
        throw new UnsupportedOperationException("get");
    }
}
