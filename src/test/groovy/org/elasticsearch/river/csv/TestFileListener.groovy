package org.elasticsearch.river.csv

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
    void onFileProcessed() {
        fileProcessed = true
    }

    @Override
    void onAllFileProcessed() {
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
}
