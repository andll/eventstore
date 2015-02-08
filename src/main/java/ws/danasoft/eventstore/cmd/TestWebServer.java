package ws.danasoft.eventstore.cmd;

import com.google.gson.stream.JsonWriter;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;
import ws.danasoft.eventstore.index.*;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.IOException;
import java.util.Optional;

public class TestWebServer {
    private static final int MAX_BOUNDARIES = Integer.getInteger("maxBoundaries", 10);

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: SeqReadTest file port");
            System.exit(1);
        }
        String fileName = args[0];
        int port = Integer.parseInt(args[1]);
        File file = new File(fileName);
        if (!file.exists()) {
            System.err.println("File does not exists");
            System.exit(2);
        }
        BTreeNodeConfiguration<Long, Long> configuration = new BTreeNodeConfiguration<>(MAX_BOUNDARIES,
                (x) -> 0l, new LongLongBTreeSerializer());
        RegionMapper regionMapper = new FseekRegionMapper(file.toPath(), configuration.elementSize());
        BTree<Long, Long> bTree = BTree.load(configuration, regionMapper);
        Server server = new Server(port);
        server.setHandler(new AbstractHandler() {
            @Override
            public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
                if (!target.equals("/z/")) {
                    return;
                }
                baseRequest.setHandled(true);
                long from;
                try {
                    from = Long.parseLong(request.getParameter("from"));
                } catch (NumberFormatException | NullPointerException e) {
                    response.sendError(400, "Illegal from value");
                    return;
                }
                long to;
                try {
                    to = Long.parseLong(request.getParameter("to"));
                } catch (NumberFormatException | NullPointerException e) {
                    response.sendError(400, "Illegal to value");
                    return;
                }
                long count;
                try {
                    count = Long.parseLong(request.getParameter("count"));
                } catch (NumberFormatException | NullPointerException e) {
                    response.sendError(400, "Illegal count value");
                    return;
                }
                if (count > 1000) {
                    response.sendError(400, "Illegal count value");
                    return;
                }
                response.setHeader("Access-Control-Allow-Origin", "*");
                long prevIndex = from;
                Optional<BTreeNode<Long, Long>> prevNode = bTree.lookup(prevIndex - 1);
                response.setContentType("application/json");
                try (JsonWriter jsonWriter = new JsonWriter(response.getWriter())) {
                    jsonWriter.setIndent(" ");
                    jsonWriter.beginArray();
                    for (long i = 1; i <= count; i++) {
                        long index = from + (to - from) / count * i;
                        Optional<BTreeNode<Long, Long>> node = bTree.lookup(index);
                        if (!node.isPresent()) {
                            break;
                        }
                        jsonWriter.beginObject()
                                .name("range").value(String.format("%d-%d", prevIndex, index))
                                .name("k").value((index + prevIndex) / 2)
                                .name("v").value(node.get().getValue() - prevNode.map(BTreeNode::getValue).orElse(0l))
                                .endObject();
                        prevNode = node;
                        prevIndex = index;
                    }
                    jsonWriter.endArray();
                }
            }
        });
        server.start();
    }
}
