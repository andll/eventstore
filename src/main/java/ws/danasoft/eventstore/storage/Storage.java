package ws.danasoft.eventstore.storage;

import com.google.common.annotations.VisibleForTesting;
import ws.danasoft.eventstore.data.DataObject;
import ws.danasoft.eventstore.data.ObjectStamp;

import java.util.*;

public class Storage {
    private final TreeMap<ObjectStamp, StorageNode> nodeMap = new TreeMap<>();

    public synchronized void add(ObjectStamp stamp, DataObject object) {
        StorageNode node;
        if (nodeMap.isEmpty()) {
            nodeMap.put(stamp, node = new StorageNode());
        } else {
            SortedMap<ObjectStamp, StorageNode> tail = nodeMap.tailMap(stamp);
            if (tail.isEmpty()) {
                node = nodeMap.lastEntry().getValue();
                if (node.size() >= StorageNode.MAX_CAPACITY) {
                    node = split(stamp, node);
                }
            } else {
                node = tail.values().iterator().next();
                if (node.size() >= StorageNode.MAX_CAPACITY) {
                    node = split(stamp, node);
                }
            }
        }
        node.add(stamp, object);
    }

    private StorageNode split(ObjectStamp stamp, StorageNode node) {
        int cmp = node.lastStamp().compareTo(stamp);
        if (cmp < 0) {
            nodeMap.put(stamp, node = new StorageNode());
        } else if (cmp == 0) {
            //Do nothing, add will overwrite value
        } else {
            nodeMap.put(stamp, node.split(stamp));
        }
        return node;
    }

    @VisibleForTesting
    List<List<StorageNode.ChunkEntry>> all() {
        List<List<StorageNode.ChunkEntry>> result = new ArrayList<>();
        for (StorageNode node : nodeMap.values()) {
            result.add(node.all());
        }
        return Collections.unmodifiableList(result);
    }
}
