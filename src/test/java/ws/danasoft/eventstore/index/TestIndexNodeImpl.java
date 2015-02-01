package ws.danasoft.eventstore.index;

import com.google.gson.stream.JsonWriter;

import java.io.IOException;
import java.util.List;

public class TestIndexNodeImpl implements IndexNode<Long, Integer> {
    private final List<Integer> boundaries;
    private final List<TestIndexNodeImpl> nodes;
    private final Long aggregatedValue;

    public TestIndexNodeImpl(List<Integer> boundaries, List<TestIndexNodeImpl> nodes, Long aggregatedValue) {
        BTreeNode.validate(boundaries, nodes);
        this.boundaries = boundaries;
        this.nodes = nodes;
        this.aggregatedValue = aggregatedValue;
    }

    @Override
    public List<TestIndexNodeImpl> nodes() {
        return nodes;
    }

    @Override
    public List<Integer> boundaries() {
        return boundaries;
    }

    @Override
    public Long aggregatedValue() {
        return aggregatedValue;
    }

    public void printTo(JsonWriter jsonWriter) throws IOException {
        jsonWriter.beginObject();
        jsonWriter.name("value").value(aggregatedValue);
        for (int i = 0; i < nodes.size(); i++) {
            if (i == nodes.size() - 1) {
                jsonWriter.name(">=" + boundaries.get(nodes.size() - 2));
            } else {
                jsonWriter.name("<" + boundaries.get(i));
            }
            nodes.get(i).printTo(jsonWriter);
        }
        jsonWriter.endObject();
    }
}
