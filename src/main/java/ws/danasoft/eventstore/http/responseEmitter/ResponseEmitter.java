package ws.danasoft.eventstore.http.responseEmitter;

import com.google.common.collect.Range;

import javax.servlet.http.HttpServletResponse;
import java.io.Closeable;
import java.io.IOException;
import java.util.Optional;

public abstract class ResponseEmitter implements Closeable {
    public abstract void init(HttpServletResponse response) throws IOException;

    public abstract void headers(String columnName) throws IOException;

    public abstract void data(Point point) throws IOException;

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

    public static class Point {
        private final long key;
        private final long value;
        private final Optional<Range<Long>> range;

        public Point(long key, long value, Optional<Range<Long>> range) {
            this.key = key;
            this.value = value;
            this.range = range;
        }

        public long getKey() {
            return key;
        }

        public long getValue() {
            return value;
        }

        public Optional<Range<Long>> getRange() {
            return range;
        }
    }
}
