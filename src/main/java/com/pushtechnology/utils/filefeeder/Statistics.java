package com.pushtechnology.utils.filefeeder;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class Statistics {
    private AtomicInteger updateCount;

    private int period = 5;
    private int lastCount = 0;

    private final ScheduledExecutorService executor;
    private final ScheduledFuture<?> scheduledFuture;

    public Statistics() {
        updateCount = new AtomicInteger(0);

        Runnable outputTask = () -> {
            int count = updateCount.get();
            int periodCount = count - lastCount;
            lastCount = count;
            System.out.println("Updates: " + count + " (" + periodCount + ", " + (periodCount / period) + "/sec)");
        };

        executor = Executors.newSingleThreadScheduledExecutor();
        scheduledFuture = executor.scheduleAtFixedRate(outputTask, 0, period, TimeUnit.SECONDS);
    }

    public AtomicInteger getUpdateCount() {
        return updateCount;
    }

    public void stop() {
        scheduledFuture.cancel(true);
        executor.shutdown();
    }
}

