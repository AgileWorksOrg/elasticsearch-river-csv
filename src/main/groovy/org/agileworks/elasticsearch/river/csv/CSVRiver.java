package org.agileworks.elasticsearch.river.csv;

import org.agileworks.elasticsearch.river.csv.listener.BashFileProcessorListener;
import org.agileworks.elasticsearch.river.csv.listener.DelegatingFileProcessorListener;
import org.agileworks.elasticsearch.river.csv.listener.FileProcessorListener;
import org.agileworks.elasticsearch.river.csv.processrunner.ProcessRunnerFactory;
import org.agileworks.elasticsearch.river.csv.processrunner.ProcessRunner;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
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

import java.io.File;

public class CSVRiver extends AbstractRiverComponent implements River, FileProcessorListener {

    private final Client client;
    private final OpenCSVFileProcessorFactory factory = new OpenCSVFileProcessorFactory();
    private Thread thread;
    private volatile boolean closed = false;
    private Configuration config;
    private BulkProcessor bulkProcessor;

    DelegatingFileProcessorListener listener;

    @SuppressWarnings({"unchecked"})
    @Inject
    public CSVRiver(RiverName riverName, RiverSettings settings, Client client) {
        super(riverName, settings);
        this.client = client;

        config = new Configuration(settings, riverName.name());

        ProcessRunner processRunner = ProcessRunnerFactory.getInstance().getRunner();

        listener = new DelegatingFileProcessorListener(this, new BashFileProcessorListener(logger, config, processRunner));
    }

    @Override
    public void start() {

        logger.info("starting csv stream");

        thread = EsExecutors.daemonThreadFactory(settings.globalSettings(), "CSV processor").newThread(new CSVConnector(listener, config, factory));
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

    public void log(String message, Object... args) {
        logger.info(message, args);
    }

    @Override
    public void log(String message) {
        logger.info(message);
    }

    @Override
    public void onBeforeProcessingStart(File[] files) {

        bulkProcessor = BulkProcessor.builder(client, new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long executionId, BulkRequest request) {
                logger.info("Going to execute new bulk composed of {} actions", request.numberOfActions());
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                logger.warn("Error executing bulk", failure);
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                logger.info("Executed bulk composed of {} actions", request.numberOfActions());
            }
        }).setBulkActions(config.getBulkSize()).setConcurrentRequests(config.getConcurrentRequests()).build();
    }

    @Override
    public void onLineProcessed(IndexRequest request) {

        logger.debug("Adding request {}", request);

        bulkProcessor.add(request);
    }

    @Override
    public void onFileProcessed(File file) {
        logger.info("File has been processed {}", file.getName());
    }

    @Override
    public void onAllFileProcessed(File[] files) {
        bulkProcessor.close();
        delay();
    }

    @Override
    public void onError(Exception e) {
        logger.error(e.getMessage(), e, (Object) null);
        closed = true;
    }

    @Override
    public void onErrorAndContinue(Exception e, String message) {
        logger.error(message, e);
    }

    @Override
    public boolean listening() {
        return !closed;
    }

    @Override
    public void onBeforeFileProcess(File file) {

    }
}
