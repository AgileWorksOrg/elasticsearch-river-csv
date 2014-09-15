package org.agileworks.elasticsearch.river.csv

import org.agileworks.elasticsearch.river.csv.listener.FileProcessorListener
import org.elasticsearch.action.index.IndexRequest

/**
 * Created by vtajzich on 21/02/14.
 */
class TestFileListener implements FileProcessorListener {

    List<IndexRequest> requests = []
    List<String> messages = []

    boolean fileProcessed
    boolean allFileProcessed
    boolean error

    @Override
    void onLineProcessed(IndexRequest request) {
        this.requests << request
    }

    @Override
    void onBeforeProcessingStart(File[] files) {

    }

    @Override
    void onFileProcessed(File file) {
        fileProcessed = true
    }

    @Override
    void onAllFileProcessed(File[] files) {
        allFileProcessed = true
    }

    @Override
    void onError(Exception e) {
        error = true
    }

    @Override
    boolean listening() {
        return true
    }

    @Override
    void log(String message, Object... args) {
        messages << message
    }

    @Override
    void log(String message) {
        messages << message
    }

    @Override
    void onErrorAndContinue(Exception e, String message) {

    }

    @Override
    void onBeforeFileProcess(File file) {

    }
}
