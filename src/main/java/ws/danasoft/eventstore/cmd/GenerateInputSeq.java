package ws.danasoft.eventstore.cmd;

import com.google.gson.stream.JsonWriter;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.Random;

public class GenerateInputSeq {
    public static void main(String[] args) throws IOException {
        if (args.length != 2) {
            System.err.println("Usage: GenerateInputSeq from count");
            System.exit(1);
        }
        long from = Long.parseLong(args[0]);
        long count = Long.parseLong(args[1]);
        Random random = new Random();
        try (Writer writer = new OutputStreamWriter(System.out);
             JsonWriter jsonWriter = new JsonWriter(writer)) {
            jsonWriter.setLenient(true);
            for (long i = 0; i <= count; i++) {
                long value = (long) (Math.log(i + from) * 3 + random.nextInt(20));
                jsonWriter.beginObject()
                        .name("time").beginObject().name("$date").value(i + from).endObject()
                        .name("value").value(value)
                        .endObject()
                        .flush();
                writer.write('\n');
            }
        }
    }
}
