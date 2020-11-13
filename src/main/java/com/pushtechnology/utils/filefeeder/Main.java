/*
 * Push Technology Ltd. ("Push") CONFIDENTIAL
 * Unpublished Copyright © 2017 Push Technology Ltd., All Rights Reserved.
 */
package com.pushtechnology.utils.filefeeder;

import com.pushtechnology.diffusion.client.Diffusion;
import com.pushtechnology.diffusion.client.callbacks.ErrorReason;
import com.pushtechnology.diffusion.client.features.TimeSeries;
import com.pushtechnology.diffusion.client.features.TopicUpdate;
import com.pushtechnology.diffusion.client.features.UpdateStream;
import com.pushtechnology.diffusion.client.features.control.topics.TopicControl;
import com.pushtechnology.diffusion.client.features.control.topics.TopicUpdateControl;
import com.pushtechnology.diffusion.client.features.control.topics.TopicUpdateControl.UpdateSource;
import com.pushtechnology.diffusion.client.features.control.topics.TopicUpdateControl.ValueUpdater;
import com.pushtechnology.diffusion.client.session.Session;
import com.pushtechnology.diffusion.client.session.SessionFactory;
import com.pushtechnology.diffusion.client.topics.details.TopicSpecification;
import com.pushtechnology.diffusion.client.topics.details.TopicType;
import com.pushtechnology.diffusion.datatype.binary.Binary;
import com.pushtechnology.diffusion.datatype.json.JSON;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import static java.util.Arrays.asList;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;

/**
 *
 * @author adam
 */
public class Main {

    public static final TopicUpdateControl.Updater.UpdateCallback DEFAULT_UPDATE_CALLBACK = new TopicUpdateControl.Updater.UpdateCallback.Default() {
        @Override
        public void onError(ErrorReason errorReason) {
            System.err.println("Update failed: " + errorReason);
        }
    };

    private final String url;
    private final String principal;
    private final String credentials;
    private final String topic;
    private final boolean topicIsJson;
    private final boolean topicDontRetain;
    private final boolean topicPublishOnly;
    private final boolean topicIsTimeSeries;
    private final String filename;
    private final long sleep;
    private final boolean repeat;
    private final boolean syncUpdates;

    private Session session;
    private UpdateStream updateStream;

    private TopicControl topicControl = null;
    private TopicUpdate topicUpdate = null;
    private TimeSeries timeSeries = null;

    private Class dataTypeClass = null;

    private List<byte[]> dataCache;

    private Statistics statistics;

    public Main(final OptionSet options) {
        url = (String) options.valueOf("url");
        principal = (String) options.valueOf("principal");
        credentials = (String) options.valueOf("credentials");
        topic = (String) options.valueOf("topic");
        topicIsJson = options.has("json");
        topicDontRetain = options.has("dontretain");
        topicPublishOnly = options.has("publishonly");
        topicIsTimeSeries = options.has("timeseries");
        filename = (String) options.valueOf("file");
        sleep = (Long) options.valueOf("sleep");
        repeat = options.has("repeat");
        syncUpdates = options.has("sync");

        dataCache = new ArrayList<>();
        statistics = new Statistics();
    }

    public void showOptions() {
        System.out.println("URL: \t\t\t\t" + url);
        System.out.println("Principal: \t\t\t" + principal);
        System.out.println("Credentials: \t\t\t" + credentials);
        System.out.println("Topic: \t\t\t\t" + topic);
        System.out.println("Topic type: \t\t\t" + (topicIsJson ? "JSON" : "Binary"));
        System.out.println("Don't retain value: \t\t" + topicDontRetain);
        System.out.println("Publish values only: \t\t" + topicPublishOnly);
        System.out.println("Time series: \t\t\t" + topicIsTimeSeries);
        System.out.println("Sleep between updates: \t\t" + sleep + "ms");
        System.out.println("Synchronous topic updates: \t" + syncUpdates);
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

        // Add topic
        TopicSpecification topicSpec;

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

        topicControl.removeTopics(">" + topic);

        try {
            topicControl.addTopic(topic, topicSpec).get();
        } catch (InterruptedException | ExecutionException ex) {
            ex.printStackTrace();
            System.exit(1);
        }

        if(! topicIsTimeSeries) {
            try {
                updateStream = session.feature(TopicUpdate.class).createUpdateStream(topic, dataTypeClass);
                updateStream.validate().get();
            } catch (Exception ex) {
                System.err.println("Unable to create/validate update stream: " + ex.getMessage());
                System.exit(1);
            }
        }
    }

    public void run() {
        System.out.println("run()");

        if (Paths.get(filename).toFile().isFile()) {
            processFile(Paths.get(filename));
        } else {
            try {
                Files.list(Paths.get(filename))
                        .filter(path -> {
                            return path.toFile().isFile();
                        })
                        .sorted()
                        .forEach(path -> {
                            processFile(path);
                            if(sleep >= 0) {
                                try {
                                    Thread.sleep(sleep);
                                } catch (InterruptedException ignore) {
                                }
                            }
                        });
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }

        if(repeat) {
            System.out.println("Data from cache");
            while(true) {
                dataCache.forEach(bytes -> {
                        if(sleep >= 0) {
                            try {
                                Thread.sleep(sleep);
                            } catch (InterruptedException ignore) {
                            }
                        }

                    updateTopic(topic, bytes);
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

    public void processFile(Path path) {
        System.out.println("Processing file: " + path);

        byte[] bytes = readFileContent(path);

        System.out.println("Got file contents");
        dataCache.add(bytes);

        updateTopic(topic, bytes);

        System.out.println("Sent update (" + bytes.length + " bytes)");
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

    private void updateTopic(final String topicPath, final byte[] bytes) {

        Object value;
        if(topicIsJson) {
            value = Diffusion.dataTypes().json().readValue(bytes);
        }
        else {
            value = Diffusion.dataTypes().binary().readValue(bytes);
        }

        CompletableFuture result;
        if (topicIsTimeSeries) {
            result = timeSeries.append(topicPath, dataTypeClass, value);
        }
        else {
            if(topicDontRetain) { // Must use TopicUpdate feature for DONT_RETAIN_TOPICS, stream not supported.
                result = topicUpdate.set(topic, dataTypeClass, value);
            }
            else {
                result = updateStream.set(value);
            }
        }
        if(syncUpdates) {
            try {
                result.get();
            } catch (InterruptedException | ExecutionException ex) {
                ex.printStackTrace();
            }
        }

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

                acceptsAll(asList("t", "topic"), "Diffusion topic name")
                        .withRequiredArg()
                        .ofType(String.class)
                        .defaultsTo("file");

                acceptsAll(asList("j", "json"), "Treat data as JSON");

                acceptsAll(asList("v", "values"), "Values only (no deltas)");

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

                acceptsAll(asList("su", "sync"), "Use synchronous updates");
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
