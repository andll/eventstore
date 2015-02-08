package ws.danasoft.eventstore.http.responseEmitter;

import java.io.IOException;

public class GoogleChartsJsonEmitter extends JsonEmitter {
    @Override
    public void headers(String columnName) throws IOException {
        jsonWriter.beginObject();
        jsonWriter.name("cols").beginArray();
        jsonWriter.beginObject().name("id").value("A").name("label").value("Num").name("type").value("number").endObject();
        jsonWriter.beginObject().name("id").value("B").name("label").value("Value").name("type").value("number").endObject();
        jsonWriter.endArray();
        jsonWriter.name("rows");
        jsonWriter.beginArray();
    }

    @Override
    public void data(long k, long v) throws IOException {
        jsonWriter.beginObject().name("c");
        jsonWriter.beginArray();
        jsonWriter.beginObject().name("v").value(k).endObject();
        jsonWriter.beginObject().name("v").value(v).endObject();
        jsonWriter.endArray();
        jsonWriter.endObject();
    }

    @Override
    public void close() throws IOException {
        jsonWriter.endArray();
        jsonWriter.endObject();
        super.close();
    }
}
