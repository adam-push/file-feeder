/*
 * Push Technology Ltd. ("Push") CONFIDENTIAL
 * Unpublished Copyright © 2017 Push Technology Ltd., All Rights Reserved.
 */
package com.pushtechnology.utils.filefeeder;

import com.pushtechnology.diffusion.client.Diffusion;
import com.pushtechnology.diffusion.client.features.TopicUpdate;
import com.pushtechnology.diffusion.client.features.UpdateStream;
import com.pushtechnology.diffusion.client.features.control.topics.TopicControl;
import com.pushtechnology.diffusion.client.session.Session;
import com.pushtechnology.diffusion.client.session.SessionFactory;
import com.pushtechnology.diffusion.client.topics.details.TopicSpecification;
import com.pushtechnology.diffusion.client.topics.details.TopicType;
import com.pushtechnology.diffusion.datatype.DataType;
import com.pushtechnology.diffusion.datatype.InvalidDataException;
import com.pushtechnology.diffusion.datatype.binary.Binary;
import com.pushtechnology.diffusion.datatype.json.JSON;
import joptsimple.OptionParser;
import joptsimple.OptionSet;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

import static java.util.Arrays.asList;

/**
 *
 * @author adam
 */
public class Main {

    private final String url;
    private final String principal;
    private final String credentials;

    private final String fixedTopicName;
    private final String topicPrefix;
    private final DataType<?> dataType;
    private final TopicType topicType;
    private final Class valueClass;

    private final boolean topicDontRetain;
    private final boolean topicPublishOnly;
    private final boolean topicIsTimeSeries;
    private final String filename;
    private final boolean splitLines;
    private final boolean deleteFiles;
    private final long sleep;
    private final boolean repeat;
    private final boolean useCache;
    private final int batchSize;
    private final boolean streamUpdates;

    private Session session;
    private final Statistics statistics;

    ArrayList<CompletableFuture<?>> futures = new ArrayList<>(100);

    private TopicControl topicControl = null;
    private TopicUpdate topicUpdate = null;

    private TopicSpecification topicSpec = null;

    private final Tree<ChunkSupplier> cache;
    private final Map<String, UpdateStream> updateStreams;

    private final Random rnd = new Random();

    public Main(final OptionSet options) {
        url = (String) options.valueOf("url");
        principal = (String) options.valueOf("principal");
        credentials = (String) options.valueOf("credentials");

        fixedTopicName = options.has("topic") ? (String)options.valueOf("topic") : null;

        String tmp = options.has("prefix") ? (String)options.valueOf("prefix") : null;
        if(tmp != null) {
            while(tmp.endsWith("/")) {
                tmp = tmp.substring(0, tmp.length() - 1);
            }
        }
        topicPrefix = tmp;
        deleteFiles = options.has("delete");

        String type = ((String)options.valueOf("type"));
        dataType = Diffusion.dataTypes().getByName(type.toLowerCase());
        topicType = TopicType.valueOf(type.toUpperCase());

        switch(topicType) {
            case STRING:
                valueClass = String.class;
                break;
            case JSON:
                valueClass = JSON.class;
                break;
            case INT64:
                valueClass = Long.class;
                break;
            case DOUBLE:
                valueClass = Double.class;
                break;
            case BINARY:
                valueClass = Binary.class;
                break;
            default:
                valueClass = null;
                break;
        }

        topicDontRetain = options.has("dontretain");
        topicPublishOnly = options.has("publishonly");
        topicIsTimeSeries = options.has("timeseries");
        filename = (String) options.valueOf("file");
        splitLines = options.has("newline");
        sleep = (Long) options.valueOf("sleep");
        repeat = options.has("repeat");
        useCache = options.has("cache");
        batchSize = (Integer) options.valueOf("batch");
        streamUpdates = options.has("stream");

        cache = new Tree<>();
        updateStreams = new HashMap<>();

        statistics = new Statistics();
    }

    public void showOptions() {
        System.out.println("URL: \t\t\t\t" + url);
        System.out.println("Principal: \t\t\t" + principal);
        System.out.println("Credentials: \t\t\t" + credentials);
        System.out.println("Topic type: \t\t\t" + topicType.name());
        System.out.println("Don't retain value: \t\t" + topicDontRetain);
        System.out.println("Publish values only: \t\t" + topicPublishOnly);
        System.out.println("Time series: \t\t\t" + topicIsTimeSeries);
        System.out.println("Sleep between updates: \t\t" + sleep + "ms");
        System.out.println("Batch size (outstanding ACKs): \t" + batchSize);
        System.out.println("Updating using stream: \t\t" + streamUpdates);
        System.out.println("Read data from: \t\t" + filename);
        System.out.println("Repeat forever: \t\t" + repeat);
    }

    public void connect() {
        SessionFactory factory = Diffusion.sessions();
        if (principal != null) {
            factory = factory.principal(principal);
        }
        if (credentials != null) {
            factory = factory.password(credentials);
        }

        while (true) {
            session = factory.open(url);
            if (session != null && session.getState().isConnected()) {
                break;
            }
            try {
                System.out.println("Unable to connect, retrying");
                Thread.sleep(1000);
            } catch (InterruptedException ignore) {
            }
        }
    }

    public void init() {
        System.out.println("init()");
        topicControl = session.feature(TopicControl.class);
        topicUpdate = session.feature(TopicUpdate.class);

        if (topicIsTimeSeries) {
            topicSpec = topicControl.newSpecification(TopicType.TIME_SERIES);
            topicSpec = topicSpec.withProperty(TopicSpecification.TIME_SERIES_EVENT_VALUE_TYPE, topicType.name());
            topicSpec = topicSpec.withProperty(TopicSpecification.TIME_SERIES_RETAINED_RANGE, "limit " + 1000); // Integer.MAX_VALUE);
        } else {
            topicSpec = topicControl.newSpecification(topicType);
        }

        if (topicDontRetain) {
            System.out.println("Topic has property DONT_RETAIN_VALUE");
            topicSpec = topicSpec.withProperty(TopicSpecification.DONT_RETAIN_VALUE, "true");
        }

        if (topicPublishOnly) {
            System.out.println("Topic has property PUBLISH_VALUES_ONLY");
            topicSpec = topicSpec.withProperty(TopicSpecification.PUBLISH_VALUES_ONLY, "true");
        }
    }

    private String calculateTopicName(String topicName) {
        String topic = fixedTopicName != null ? fixedTopicName : topicName;
        if(topicPrefix != null) {
            topic = topicPrefix + "/" + topic;
        }
        return topic;
    }

    private void createTopic(String topicName) {
        String topic = calculateTopicName(topicName);
        System.out.println("Create topic: \"" + topic + "\"");
        // topicControl.removeTopics(">" + topicName);

        try {
            topicControl.addTopic(topic, topicSpec).get();
        } catch (InterruptedException | ExecutionException ex) {
            ex.printStackTrace();
        }
    }

    private void createUpdateStream(String topicName) {
        String topic = calculateTopicName(topicName);

        if(updateStreams.containsKey(topic)) {
            return;
        }

        if (!topicIsTimeSeries && streamUpdates) {
            try {
                UpdateStream<?> updateStream = session.feature(TopicUpdate.class).createUpdateStream(topic, dataType.getClass());
                updateStream.validate().get();
                updateStreams.put(topic, updateStream);
            } catch (Exception ex) {
                System.err.println("Unable to create/validate update stream for \"" + topic + "\": " + ex.getMessage());
                System.exit(1);
            }
        }
    }

    private void walk(Path dir) {
        try {
            Stream<Path> fileStream = Files.list(dir);
            fileStream.filter(path -> path.toFile().isFile())
                    .sorted()
                    .forEach(path -> {
                        try {
                            String topicName = pathToTopicName(path);
                            createTopic(topicName);
                            createUpdateStream(topicName);
                            processFile(path);
                        }
                        catch(IOException ex) {
                            ex.printStackTrace();
                        }
                    });
            fileStream.close();

            fileStream = Files.list(dir);
            fileStream.filter(path -> path.toFile().isDirectory())
                    .sorted()
                    .forEach(this::walk);
            fileStream.close();
        } catch (IOException ex) {
            ex.printStackTrace();

            try {
                Thread.sleep(60000);
            }
            catch(InterruptedException ignore) {}
            System.exit(1);
        }
    }

    public void run() {
        System.out.println("run()");

        boolean firstRun = true;

        do {
            if(firstRun || ! useCache) {
                if (Paths.get(filename).toFile().isFile()) {
                    processFile(Paths.get(filename));
                } else {
                    walk(Paths.get(filename));
                }
            }
            else {
                // Using cache
                LinkedList<Tree<ChunkSupplier>.TreeNode<ChunkSupplier>> nodesWithData = cache.getNodesWithData();

                Collections.shuffle(nodesWithData);
                nodesWithData.forEach(node -> {
                    if (sleep >= 0) {
                        try {
                            Thread.sleep(sleep);
                        } catch (InterruptedException ignore) {
                        }
                    }
                    String topicName = cache.getFullName(node);

                    // Get some random data for this node.
                    // If there are multiple records per file, choose a random record from this file.
                    // Otherwise, find a topic at the same level (i.e. a sibling) and use its data.
                    // Failing all of that, just use our own data again.
                    byte[] data = null;
                    if(splitLines) {
                        data = node.data.getRandom();
                    }
                    else {
                        // Choose a random sibling to get data for. If there are no other siblings, just use own
                        // own data again.
                        LinkedList<Tree<ChunkSupplier>.TreeNode<ChunkSupplier>> siblings = cache.getSiblings(node);
                        if(siblings.size() > 0) {
                            int i = rnd.nextInt(siblings.size());
                            data = siblings.get(i).data.get();
                        }
                        else {
                            if(data == null) {
                                data = node.data.get();
                            }
                        }
                    }

                    updateTopic(topicName, data);
                });
            }

            firstRun = false;

        } while(repeat);

        statistics.stop();
        session.close();
    }

    public void stop() {
        System.out.println("stop()");

        statistics.stop();
        if(session != null) {
            session.close();
            System.out.println("Session closed");
        }
    }

    private String pathToTopicName(Path path) throws IOException {
        String leading = new File(filename).getCanonicalFile().getParent();
        String name = new File(path.toFile().getCanonicalFile().toString().substring(leading.length() + 1)).toString();
        int idx = name.lastIndexOf('.');
        if(idx != -1) {
            name = name.substring(0, idx);
        }
        return name;
    }

    public void processFile(Path path) {
        System.out.println("Processing file: " + path);

        byte[] bytes = readFileContent(path);

        try {
            String topicName = pathToTopicName(path);

            ChunkSupplier supplier = new ChunkSupplier(bytes, splitLines);

            if(useCache) {
                cache.add(topicName, supplier);
            }

            byte[] chunk;
            while((chunk = supplier.get()) != null) {
                updateTopic(topicName, chunk);

                if(sleep >= 0) {
                    try {
                        Thread.sleep(sleep);
                    } catch (InterruptedException ignore) {
                    }
                }
            }

            if(deleteFiles) {
                if(path.toFile().delete()) {
                    System.out.println("Deleted file " + path);
                }
                else {
                    System.out.println("Failed to delete file " + path);
                }
            }
        }
        catch(IOException ex) {
            System.err.println("Unable to process file: " + ex.getMessage());
        }

    }

    private byte[] readFileContent(final Path path) {
        byte[] bytes = null;
        try {
            bytes = Files.readAllBytes(path);
        } catch (IOException ex) {
            System.err.println("IOException while reading " + path + ": " + ex.getMessage());
        }
        return bytes;
    }

    private void waitForFutures() {
        CompletableFuture<?> future = futures.get(0);

        try {
            // Wait until we've got it
            future.get();
        } catch (InterruptedException | ExecutionException ex) {
            ex.printStackTrace();
        }

        // Remove any other "done" futures from the list
        while(futures.size() > 0) {
            future = futures.get(0);
            if (future.isDone()) {
                futures.remove(0);
            }
            else {
                break;
            }
        }
    }

    private void updateTopic(final String topicPath, final byte[] bytes) {
        // Don't update if there are too many futures outstanding
        if(futures.size() >= batchSize) {
            waitForFutures();
        }

        Object value = null;

        try {
            switch (topicType) {
                case STRING:
                    value = new String(bytes);
                    break;
                case JSON:
                    value = Diffusion.dataTypes().json().fromJsonString(new String(bytes));
                    break;
                case BINARY:
                    value = Diffusion.dataTypes().binary().readValue(bytes);
                    break;
                case RECORD_V2:
                    value = Diffusion.dataTypes().recordV2().readValue(bytes);
                    break;
                case DOUBLE:
                    value = Diffusion.dataTypes().doubleFloat().readValue(bytes);
                    break;
                case INT64:
                    value = Diffusion.dataTypes().int64().readValue(bytes);
                    break;
                default:
                    break;
            }
        }
        catch(InvalidDataException ex) {
            System.err.println("Invalid data:" + ex.getMessage());
        }

        if(value == null) {
            System.err.println("Value for " + topicPath + " is null, ignoring");
            return;
        }

        String topic = calculateTopicName(topicPath);

        CompletableFuture<?> result;
        if(streamUpdates) {
            result = updateStreams.get(topic).set(value);
        }
        else {
            result = topicUpdate.set(topic, valueClass, value);
        }

        futures.add(result);

        statistics.getUpdateCount().incrementAndGet();
    }

    public static void main(final String[] args) throws Exception {
        OptionParser optionParser = new OptionParser() {
            {
                acceptsAll(asList("h", "help"),
                        "This help");

                acceptsAll(asList("u", "url"), "Diffusion URL")
                        .withRequiredArg()
                        .ofType(String.class)
                        .defaultsTo("ws://localhost:8080");

                acceptsAll(asList("p", "principal"), "Diffusion principal")
                        .withRequiredArg()
                        .ofType(String.class)
                        .defaultsTo("control");

                acceptsAll(asList("c", "credentials"), "Diffusion credentials (password)")
                        .withRequiredArg()
                        .ofType(String.class)
                        .defaultsTo("password");

                acceptsAll(asList("t", "type"), "Default data type (JSON, STRING, INT64, DOUBLE, BINARY)")
                        .withRequiredArg()
                        .ofType(String.class)
                        .defaultsTo("binary");

                acceptsAll(asList("dr", "dontretain"), "Don't retain topic values");

                acceptsAll(asList("po", "publishonly"), "Publish values only (no deltas)");

                acceptsAll(asList("ts", "timeseries"), "Topic is time-series");

                acceptsAll(asList("f", "file"), "File, or directory containing files to process")
                        .withRequiredArg()
                        .ofType(String.class)
                        .defaultsTo("files");

                acceptsAll(asList("nl", "newline"), "Files contain multiple newline-delimited records. Process them individually.");

                acceptsAll(asList("s", "sleep"), "Time to sleep between each file (in ms)")
                        .withRequiredArg()
                        .ofType(Long.class)
                        .defaultsTo(1000L);

                acceptsAll(asList("r", "repeat"), "Repeat after all files processed");

                acceptsAll(asList("cache"), "Cache file data for use when --repeat is specified; do not read the file contents again");

                acceptsAll(asList("stream"), "Use UpdateStream (not for timeseries)");

                acceptsAll(asList("batch"), "Number of outstanding update ACKs, default unlimited (0/1 = sync)")
                        .withRequiredArg()
                        .ofType(Integer.class)
                        .defaultsTo(Integer.MAX_VALUE);

                acceptsAll(asList("topic"), "Fixed topic name")
                        .withRequiredArg()
                        .ofType(String.class);

                acceptsAll(asList("prefix"), "Topic prefix, prepended to all topics")
                        .withRequiredArg()
                        .ofType(String.class);

                acceptsAll(asList("delete"), "Delete file after reading");
            }
        };
        OptionSet options = optionParser.parse(args);
        if (options.has("help")) {
            optionParser.printHelpOn(System.out);
            System.exit(0);
        }

        Main app = new Main(options);
        app.showOptions();

        Runtime.getRuntime().addShutdownHook(new Thread(app::stop));

        app.connect();
        app.init();
        app.run();
    }

}
