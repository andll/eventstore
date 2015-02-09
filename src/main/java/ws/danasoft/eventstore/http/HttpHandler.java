package ws.danasoft.eventstore.http;

import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import com.google.gson.TypeAdapter;
import com.google.gson.annotations.SerializedName;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.MalformedJsonException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.server.handler.ResourceHandler;
import org.eclipse.jetty.util.resource.Resource;
import ws.danasoft.eventstore.cmd.GenerateIntegralIndex;
import ws.danasoft.eventstore.http.responseEmitter.ResponseEmitter;
import ws.danasoft.eventstore.index.*;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class HttpHandler extends AbstractHandler {
    private static final Logger LOGGER = LogManager.getLogger(HttpHandler.class);
    private static final Gson GSON = new Gson();
    private static final String BTREE_EXTENSION = ".btree";
    private static final String CONFIG_EXTENSION = ".config";

    public static void main(String[] args) throws Exception {
        int port = Integer.getInteger("port", 8080);
        Server server = new Server(port);
        HttpHandler serviceHandler = new HttpHandler();
        serviceHandler.init();
        ResourceHandler contentHandler = new ResourceHandler();
        contentHandler.setBaseResource(Resource.newClassPathResource("plot-www"));
        HandlerList handlerList = new HandlerList();
        handlerList.addHandler(contentHandler);
        handlerList.addHandler(serviceHandler);
        server.setHandler(handlerList);
        server.start();
    }

    private final Map<String, BTree<Long, Long>> fileMap = new HashMap<>(); //Read only so not synchronized

    public void init() {
        enlist(new File("."), "");
        if (fileMap.isEmpty()) {
            LOGGER.info("No files found, generating /sample sequence");
            fileMap.put("/sample", generateSample());
        } else {
            LOGGER.debug("Loaded files: {}", fileMap.keySet());
        }
    }

    private BTree<Long, Long> generateSample() {
        BTreeNodeConfiguration<Long, Long> configuration = new BTreeNodeConfiguration<>(10, (x) -> 0l, new LongLongBTreeSerializer());
        BTree<Long, Long> bTree = BTree.createNew(configuration, new MemoryRegionMapper(configuration.elementSize()));
        GenerateIntegralIndex.generateTo(0, 10000, bTree);
        return bTree;
    }

    private void enlist(File directory, String prefix) {
        File[] files = directory.listFiles();
        if (files == null) {
            throw new RuntimeException("Can not list: " + directory);
        }
        for (File file : files) {
            if (file.getName().equals(".") || file.getName().equals("..")) {
                continue;
            }
            if (file.isHidden()) {
                LOGGER.debug("Skipping hidden file {}", file);
                continue;
            }
            String name = prefix + "/" + file.getName();
            if (file.isFile()) {
                if (!file.getName().endsWith(BTREE_EXTENSION)) {
                    continue;
                }
                File configFile = new File(file + CONFIG_EXTENSION);
                if (!configFile.exists()) {
                    LOGGER.warn("Configuration file " + file + " does not exists");
                    continue;
                }
                try {
                    BTreeNodeConfiguration<Long, Long> configuration = loadConfiguration(configFile);
                    BTree<Long, Long> bTree = BTree.load(configuration, new FseekRegionMapper(file.toPath(), configuration.elementSize()));
                    fileMap.put(name.substring(0, name.length() - BTREE_EXTENSION.length()), bTree);
                } catch (IOException e) {
                    throw new RuntimeException("Failed to load file " + file, e);
                }
            } else if (file.isDirectory()) {
                enlist(file, name);
            } else {
                throw new RuntimeException("Do not know what to do with " + file + " which is not directory or file");
            }
        }
    }

    @Override
    public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
        if (target.startsWith("/images")) {
            return;
        }
        baseRequest.setHandled(true);
        BTree<Long, Long> bTree = fileMap.get(removeTrailing(target));
        if (bTree == null) {
            response.sendError(HttpServletResponse.SC_NOT_FOUND, "Sequence not found");
            return;
        }
        response.setHeader("Access-Control-Allow-Origin", "*");
        //TODO Access-Control-Allow-Method

        switch (request.getMethod()) {
            case "POST":
                switch (request.getQueryString()) {
                    case "query":
                        handleQuery(bTree, baseRequest, response);
                        return;
                    default:
                        response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Unknown command");
                }
            case "OPTIONS":
                return;
            case "GET":
            case "DELETE":
            case "PUT":
                response.sendError(HttpServletResponse.SC_FORBIDDEN);
                return;
            default:
                response.sendError(HttpServletResponse.SC_METHOD_NOT_ALLOWED);
        }
    }

    private static String removeTrailing(String target) {
        if (target.endsWith("/")) {
            return target.substring(0, target.length() - 1);
        }
        return target;
    }

    private void handleQuery(BTree<Long, Long> bTree, Request request, HttpServletResponse response) throws IOException {
        QueryRequest queryRequest;
        try {
            try (JsonReader jsonReader = new JsonReader(request.getReader())) {
                queryRequest = QueryRequest.TYPE_ADAPTER.read(jsonReader);
            }
        } catch (MalformedJsonException e) {
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Malformed json");
            return;
        } catch (IllegalStateException e) {
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Incorrect json request");
            return;
        }
        try {
            queryRequest.validate();
        } catch (IllegalStateException e) {
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, e.getMessage());
            return;
        }

        long prevIndex = queryRequest.from;
        Optional<BTreeNode<Long, Long>> prevNode = bTree.lookup(prevIndex - 1);

        try (ResponseEmitter emitter = ResponseEmitter.of(Optional.ofNullable(queryRequest.emitter))) {
            emitter.init(response);
            emitter.headers("Data");
            for (long i = 1; i <= queryRequest.groupsCount; i++) {
                long index = queryRequest.from + (queryRequest.to - queryRequest.from) / queryRequest.groupsCount * i;
                Optional<BTreeNode<Long, Long>> node = bTree.lookup(index);
                if (!node.isPresent()) {
                    break;
                }
                long value = node.get().getValue() - prevNode.map(BTreeNode::getValue).orElse(0l);
                emitter.data((index + prevIndex) / 2, value);
                prevNode = node;
                prevIndex = index;
            }
        }
    }

    private BTreeNodeConfiguration<Long, Long> loadConfiguration(File file) throws IOException {
        ConfigurationFile configurationFile;
        try (JsonReader jsonReader = new JsonReader(new FileReader(file))) {
            configurationFile = ConfigurationFile.TYPE_ADAPTER.read(jsonReader);
        }
        return new BTreeNodeConfiguration<>(configurationFile.maxBoundaries, (x) -> 0l, new LongLongBTreeSerializer());
    }

    @SuppressWarnings("UnusedDeclaration")
    private static class ConfigurationFile {
        private static final TypeAdapter<ConfigurationFile> TYPE_ADAPTER = GSON.getAdapter(ConfigurationFile.class);

        private int maxBoundaries;
    }

    @SuppressWarnings("UnusedDeclaration")
    private static class QueryRequest {
        private static final TypeAdapter<QueryRequest> TYPE_ADAPTER = GSON.getAdapter(QueryRequest.class);
        @SerializedName("$from") private Long from;
        @SerializedName("$to") private Long to;
        @SerializedName("$groupsCount") private Long groupsCount;
        @SerializedName("$emitter") private String emitter;

        public void validate() throws IllegalStateException {
            Preconditions.checkState(from != null, "$from parameter missing");
            Preconditions.checkState(to != null, "$to parameter missing");
            Preconditions.checkState(groupsCount != null, "$groupsCount parameter missing");
            Preconditions.checkState(groupsCount > 0, "$groupsCount should be positive");
            Preconditions.checkState(groupsCount <= 1000, "$groupsCount should not be greater then 1000");
        }
    }
}
