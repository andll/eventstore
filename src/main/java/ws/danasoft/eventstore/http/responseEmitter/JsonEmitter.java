package ws.danasoft.eventstore.http.responseEmitter;

import com.google.gson.stream.JsonWriter;

import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

public abstract class JsonEmitter extends ResponseEmitter {
    protected JsonWriter jsonWriter;

    @Override
    public void init(HttpServletResponse response) throws IOException {
        response.setContentType("application/json");
        jsonWriter = new JsonWriter(response.getWriter());
        jsonWriter.setIndent(" ");
    }

    @Override
    public void close() throws IOException {
        jsonWriter.close();
    }
}
