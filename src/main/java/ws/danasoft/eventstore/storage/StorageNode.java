package ws.danasoft.eventstore.storage;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import ws.danasoft.eventstore.data.DataObject;
import ws.danasoft.eventstore.data.ObjectStamp;

import java.util.*;

public class StorageNode {
    static final int MAX_CAPACITY = Integer.getInteger("StorageNode.maxCapacity", 1000);

    private final List<ChunkEntry> entries = new ArrayList<>(MAX_CAPACITY);

    public synchronized void add(ObjectStamp stamp, DataObject object) {
        ChunkEntry e = new ChunkEntry(stamp, object);
        if (entries.isEmpty() || entries.get(entries.size() - 1).compareTo(e) < 0) {
            entries.add(e);
            return;
        }
        int i = Collections.binarySearch(entries, e);
        if (i < 0) {
            entries.add(-(i + 1), e);
        } else {
            entries.set(i, e); //?
        }
    }

    public synchronized void remove(ObjectStamp stamp) throws NoSuchElementException {
        int i = Collections.binarySearch(entries, new ChunkEntry(stamp));
        if (i < 0) {
            throw new NoSuchElementException();
        }
        entries.remove(i);
    }

    synchronized ObjectStamp lastStamp() throws NoSuchElementException {
        if (entries.isEmpty()) {
            throw new NoSuchElementException();
        }
        ChunkEntry lastEntry = entries.get(entries.size() - 1);
        return lastEntry.getStamp();
    }

    @VisibleForTesting
    synchronized List<ChunkEntry> all() {
        return ImmutableList.copyOf(entries);
    }

    synchronized int size() {
        return entries.size();
    }

    /**
     * Split this node into two nodes by stamp
     * All values lower then stamp remains in this node
     * All values greater or equal to stamp are put to new node
     */
    synchronized StorageNode split(ObjectStamp stamp) {
        int i = Collections.binarySearch(entries, new ChunkEntry(stamp));
        List<ChunkEntry> tailList;
        if (i < 0) {
            tailList = entries.subList(-(i + 1), entries.size());
        } else {
            tailList = entries.subList(i, entries.size());
        }
        StorageNode newNode = new StorageNode();
        newNode.entries.addAll(tailList);
        tailList.clear();
        return newNode;
    }

    static class ChunkEntry implements Comparable<ChunkEntry> {
        private final ObjectStamp stamp;
        private final Optional<DataObject> object;

        public ChunkEntry(ObjectStamp stamp) {
            this.stamp = stamp;
            this.object = Optional.empty();
        }

        public ChunkEntry(ObjectStamp stamp, DataObject object) {
            this.stamp = stamp;
            this.object = Optional.of(object);
        }

        public ObjectStamp getStamp() {
            return stamp;
        }

        public DataObject getObject() {
            return object.get();
        }

        @Override
        public int compareTo(ChunkEntry o) {
            return stamp.compareTo(o.stamp);
        }

        @Override
        @VisibleForTesting //Equals and hashCode for unit tests only, does not meet compareTo/equals contract
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            ChunkEntry that = (ChunkEntry) o;

            if (!object.equals(that.object)) return false;
            if (!stamp.equals(that.stamp)) return false;

            return true;
        }

        @Override
        @VisibleForTesting
        public int hashCode() {
            int result = stamp.hashCode();
            result = 31 * result + object.hashCode();
            return result;
        }

        @Override
        public String toString() {
            if (object.isPresent()) {
                return object.get().toString();
            }
            return String.format("Marker for %s", stamp);
        }
    }
}
