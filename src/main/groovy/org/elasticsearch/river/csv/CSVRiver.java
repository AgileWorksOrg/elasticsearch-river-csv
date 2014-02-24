package org.elasticsearch.river.csv;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.river.AbstractRiverComponent;
import org.elasticsearch.river.River;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;
import org.elasticsearch.threadpool.ThreadPool;

public class CSVRiver extends AbstractRiverComponent implements River, FileProcessorListener {

    private final ThreadPool threadPool;
    private final Client client;
    private final OpenCSVFileProcessorFactory factory = new OpenCSVFileProcessorFactory();
    private Thread thread;
    private volatile BulkRequestBuilder currentRequest;
    private volatile boolean closed = false;
    private Configuration config;

    @SuppressWarnings({"unchecked"})
    @Inject
    public CSVRiver(RiverName riverName, RiverSettings settings, Client client, ThreadPool threadPool) {
        super(riverName, settings);
        this.client = client;
        this.threadPool = threadPool;

        config = new Configuration(settings, riverName.name());
    }

    @Override
    public void start() {

        logger.info("starting csv stream");

        currentRequest = client.prepareBulk();
        thread = EsExecutors.daemonThreadFactory(settings.globalSettings(), "CSV processor").newThread(new CSVConnector(this, config, factory));
        thread.start();
    }

    @Override
    public void close() {
        logger.info("closing csv stream river");
        this.closed = true;
        thread.interrupt();
    }

    public void delay() {
        if (config.getPoll().millis() > 0L) {
            logger.info("next run waiting for {}", config.getPoll());
            try {
                Thread.sleep(config.getPoll().millis());
            } catch (InterruptedException e) {
                logger.error("Error during waiting.", e, (Object) null);
            }

        }

    }

    public void processBulkIfNeeded(boolean force) {

        if (currentRequest.numberOfActions() >= config.getBulkSize() || (currentRequest.numberOfActions() > 0 && force)) {
            // execute the bulk operation
            int currentOnGoingBulks = config.getOnGoingBulks().incrementAndGet();
            if (currentOnGoingBulks > config.getBulkThreshold()) {
                config.getOnGoingBulks().decrementAndGet();
                logger.warn("ongoing bulk, {} crossed threshold {}, waiting", config.getOnGoingBulks(), config.getBulkThreshold());
                try {
                    synchronized (this) {
                        wait();
                    }

                } catch (InterruptedException e) {
                    logger.error("Error during wait", e);
                }

            }


            try {
                currentRequest.execute(new ActionListener<BulkResponse>() {
                    @Override
                    public void onResponse(BulkResponse bulkResponse) {
                        config.getOnGoingBulks().decrementAndGet();
                        notifyCSVRiver();
                    }

                    @Override
                    public void onFailure(Throwable e) {
                        config.getOnGoingBulks().decrementAndGet();
                        notifyCSVRiver();
                        logger.error("failed to execute bulk", e);
                    }

                });
            } catch (Exception e) {
                config.getOnGoingBulks().decrementAndGet();
                notifyCSVRiver();
                logger.warn("failed to process bulk", e);
            }


            currentRequest = client.prepareBulk();
        }

    }

    public void log(String message, Object... args) {
        logger.info(message, args);
    }

    private void notifyCSVRiver() {

        synchronized (CSVRiver.this) {
            CSVRiver.this.notify();
        }
    }

    @Override
    public void onLineProcessed(IndexRequest request) {

        logger.debug("Adding request {}", request);

        currentRequest.add(request);
        processBulkIfNeeded(false);
    }

    @Override
    public void onFileProcessed() {
        processBulkIfNeeded(false);
    }

    @Override
    public void onAllFileProcessed() {
        processBulkIfNeeded(true);
        delay();
    }

    @Override
    public void onError(Exception e) {
        logger.error(e.getMessage(), e, (Object) null);
        closed = true;
    }

    @Override
    public boolean listening() {
        return !closed;
    }
}
