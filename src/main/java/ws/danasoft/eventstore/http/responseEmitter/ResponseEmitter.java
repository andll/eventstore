package ws.danasoft.eventstore.http.responseEmitter;

import javax.servlet.http.HttpServletResponse;
import java.io.Closeable;
import java.io.IOException;
import java.util.Optional;

public abstract class ResponseEmitter implements Closeable {
    public abstract void init(HttpServletResponse response) throws IOException;

    public abstract void headers(String columnName) throws IOException;

    public abstract void data(long k, long v) throws IOException;

    @Override
    public void close() throws IOException {

    }

    public static ResponseEmitter of(Optional<String> emitter) throws IllegalArgumentException {
        if (!emitter.isPresent()) {
            return new StandardJsonEmitter();
        }
        switch (emitter.orElse("standard")) {
            case "standard":
                return new StandardJsonEmitter();
            case "google_charts":
                return new GoogleChartsJsonEmitter();
            default:
                throw new IllegalArgumentException("Unknown emitter");
        }
    }
}
