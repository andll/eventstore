package ws.danasoft.eventstore.http.responseEmitter;

import java.io.IOException;

public class StandardJsonEmitter extends JsonEmitter {
    @Override
    public void headers(String columnName) throws IOException {
        jsonWriter.beginArray();
    }

    @Override
    public void data(long k, long v) throws IOException {
        jsonWriter.beginObject()
                .name("k").value(k)
                .name("v").value(v)
                .endObject();
    }

    @Override
    public void close() throws IOException {
        jsonWriter.endArray();
        super.close();
    }
}
