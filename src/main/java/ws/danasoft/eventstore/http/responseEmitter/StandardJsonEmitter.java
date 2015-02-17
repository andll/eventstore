package ws.danasoft.eventstore.http.responseEmitter;

import com.google.common.collect.Range;

import java.io.IOException;

public class StandardJsonEmitter extends JsonEmitter {
    @Override
    public void headers(String columnName) throws IOException {
        jsonWriter.beginArray();
    }

    @Override
    public void data(Point point) throws IOException {
        jsonWriter.beginObject();
        jsonWriter.name("k").value(point.getKey());
        jsonWriter.name("v").value(point.getValue());
        if (point.getRange().isPresent()) {
            Range<Long> range = point.getRange().get();
            jsonWriter.name("range").beginObject()
                    .name("from").value(range.lowerEndpoint())
                    .name("to").value(range.upperEndpoint())
                    .endObject();
        }
        jsonWriter.endObject();
    }

    @Override
    public void close() throws IOException {
        jsonWriter.endArray();
        super.close();
    }
}
