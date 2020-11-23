/*
 * Push Technology Ltd. ("Push") CONFIDENTIAL
 * Unpublished Copyright © 2017 Push Technology Ltd., All Rights Reserved.
 */
package com.pushtechnology.utils.filefeeder;

import com.pushtechnology.diffusion.client.Diffusion;
import com.pushtechnology.diffusion.client.features.TimeSeries;
import com.pushtechnology.diffusion.client.features.TopicUpdate;
import com.pushtechnology.diffusion.client.features.UpdateStream;
import com.pushtechnology.diffusion.client.features.control.topics.TopicControl;
import com.pushtechnology.diffusion.client.session.Session;
import com.pushtechnology.diffusion.client.session.SessionFactory;
import com.pushtechnology.diffusion.client.topics.details.TopicSpecification;
import com.pushtechnology.diffusion.client.topics.details.TopicType;
import com.pushtechnology.diffusion.datatype.binary.Binary;
import com.pushtechnology.diffusion.datatype.json.JSON;
import joptsimple.OptionParser;
import joptsimple.OptionSet;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static java.util.Arrays.asList;

/**
 *
 * @author adam
 */
public class Main {

    private final String url;
    private final String principal;
    private final String credentials;
    private final boolean topicIsJson;
    private final boolean topicDontRetain;
    private final boolean topicPublishOnly;
    private final boolean topicIsTimeSeries;
    private final String filename;
    private final long sleep;
    private final boolean repeat;
    private final int batchSize;
    private final boolean streamUpdates;

    private Session session;
    private final Statistics statistics;

    ArrayList<CompletableFuture> futures = new ArrayList<>(100);

    private TopicControl topicControl = null;
    private TopicUpdate topicUpdate = null;
    private TimeSeries timeSeries = null;

    private Class dataTypeClass = null;
    private TopicSpecification topicSpec = null;

    private Tree<byte[]> cache;
    private Map<String, UpdateStream> updateStreams;

    private final Random rnd = new Random();

    public Main(final OptionSet options) {
        url = (String) options.valueOf("url");
        principal = (String) options.valueOf("principal");
        credentials = (String) options.valueOf("credentials");
        topicIsJson = options.has("json");
        topicDontRetain = options.has("dontretain");
        topicPublishOnly = options.has("publishonly");
        topicIsTimeSeries = options.has("timeseries");
        filename = (String) options.valueOf("file");
        sleep = (Long) options.valueOf("sleep");
        repeat = options.has("repeat");
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
        System.out.println("Topic type: \t\t\t" + (topicIsJson ? "JSON" : "Binary"));
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

        String reason;
        while (true) {
            reason = null;
            session = factory.open(url);
            if (session != null && session.getState().isConnected()) {
                break;
            }
            try {
                System.out.println("Unable to connect"
                        + reason != null ? ": " + reason : ""
                                + ", retrying");
                Thread.sleep(1000);
            } catch (InterruptedException ignore) {
            }
        }
    }

    public void init() {
        System.out.println("init()");
        topicControl = session.feature(TopicControl.class);
        topicUpdate = session.feature(TopicUpdate.class);
        timeSeries = session.feature(TimeSeries.class);

        // Build topic specification
        if (topicIsJson) {
            dataTypeClass = JSON.class;
        } else {
            dataTypeClass = Binary.class;
        }

        if (topicIsTimeSeries) {
            topicSpec = topicControl.newSpecification(TopicType.TIME_SERIES);
            topicSpec = topicSpec.withProperty(TopicSpecification.TIME_SERIES_EVENT_VALUE_TYPE, topicIsJson ? "json" : "binary");
            topicSpec = topicSpec.withProperty(TopicSpecification.TIME_SERIES_RETAINED_RANGE, "limit " + 1000); // Integer.MAX_VALUE);
        } else {
            topicSpec = topicControl.newSpecification(topicIsJson ? TopicType.JSON : TopicType.BINARY);
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

    private void createTopic(String topicName) {
        System.out.println("Create topic: \"" + topicName + "\"");
        topicControl.removeTopics(">" + topicName);

        try {
            topicControl.addTopic(topicName, topicSpec).get();
        } catch (InterruptedException | ExecutionException ex) {
            ex.printStackTrace();
        }
    }

    private void createUpdateStream(String topicName) {
        if (!topicIsTimeSeries && streamUpdates) {
            try {
                UpdateStream updateStream = session.feature(TopicUpdate.class).createUpdateStream(topicName, dataTypeClass);
                updateStream.validate().get();
                updateStreams.put(topicName, updateStream);
            } catch (Exception ex) {
                System.err.println("Unable to create/validate update stream for \"" + topicName + "\": " + ex.getMessage());
                System.exit(1);
            }
        }
    }

    private void walk(Path dir) {
        try {
            Files.list(dir)
                    .filter(path -> path.toFile().isFile())
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
                        if(sleep >= 0) {
                            try {
                                Thread.sleep(sleep);
                            } catch (InterruptedException ignore) {
                            }
                        }
                    });
            Files.list(dir)
                    .filter(path -> path.toFile().isDirectory())
                    .sorted()
                    .forEach(path -> {
                        walk(path);
                    });
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    public void run() {
        System.out.println("run()");

        if (Paths.get(filename).toFile().isFile()) {
            processFile(Paths.get(filename));
        } else {
            walk(Paths.get(filename));
        }

        if (repeat) {
            System.out.println("Data from cache");
            LinkedList<Tree<byte[]>.TreeNode<byte[]>> nodesWithData = cache.getNodesWithData();

            while (true) {
                Collections.shuffle(nodesWithData);
                nodesWithData.forEach(node -> {
                    if (sleep >= 0) {
                        try {
                            Thread.sleep(sleep);
                        } catch (InterruptedException ignore) {
                        }
                    }
                    String topicName = cache.getFullName(node);

                    // Choose a random sibling to get data for. If there are no other siblings, just use own
                    // own data again.
                    LinkedList<Tree<byte[]>.TreeNode<byte[]>> siblings = cache.getSiblings(node);
                    byte[] data = null;
                    if(siblings.size() > 0) {
                        int i = rnd.nextInt(siblings.size());
                        data = siblings.get(i).data;
                    }

                    if(data == null) {
                        data = node.data;
                    }

                    updateTopic(topicName, data);

                });
            }
        }
    }

    public void stop() {
        System.out.println("stop()");

        if(session != null) {
            session.close();
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

        System.out.println("Got file contents");

        try {
            String topicName = pathToTopicName(path);
            cache.add(topicName, bytes);
            updateTopic(topicName, bytes);
            System.out.println("Sent update (" + bytes.length + " bytes)");
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
        CompletableFuture future = futures.get(0);

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

        Object value;
        if(topicIsJson) {
            value = Diffusion.dataTypes().json().fromJsonString(new String(bytes));
        }
        else {
            value = Diffusion.dataTypes().binary().readValue(bytes);
        }

        CompletableFuture result;
        if (topicIsTimeSeries) {
            result = timeSeries.append(topicPath, dataTypeClass, value);
        }
        else {
            if(streamUpdates) {
                result = updateStreams.get(topicPath).set(value);
            }
            else {
                result = topicUpdate.set(topicPath, dataTypeClass, value);
            }
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

                acceptsAll(asList("j", "json"), "Treat data as JSON");

                acceptsAll(asList("dr", "dontretain"), "Don't retain topic values");

                acceptsAll(asList("po", "publishonly"), "Publish values only (no deltas)");

                acceptsAll(asList("ts", "timeseries"), "Topic is time-series");

                acceptsAll(asList("f", "file"), "File, or firectory containing files to process")
                        .withRequiredArg()
                        .ofType(String.class)
                        .defaultsTo("files");

                acceptsAll(asList("s", "sleep"), "Time to sleep between each file (in ms)")
                        .withRequiredArg()
                        .ofType(Long.class)
                        .defaultsTo(1000L);

                acceptsAll(asList("r", "repeat"), "Repeat after all files processed");

                acceptsAll(asList("stream"), "Use UpdateStream (not for timeseries)");

                acceptsAll(asList("batch"), "Number of outstanding update ACKs, default unlimited (0/1 = sync)")
                        .withRequiredArg()
                        .ofType(Integer.class)
                        .defaultsTo(Integer.MAX_VALUE);
            }
        };
        OptionSet options = optionParser.parse(args);
        if (options.has("help")) {
            optionParser.printHelpOn(System.out);
            System.exit(0);
        }

        Main app = new Main(options);
        app.showOptions();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            app.stop();
        }));

        app.connect();
        app.init();
        app.run();
    }

}
