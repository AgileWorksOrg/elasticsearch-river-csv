package org.elasticsearch.river.csv

import org.elasticsearch.action.index.IndexRequest

/**
 * It's being called from FileProcessor and CSVConnector in order
 * to notify river what's happening
 */
public interface FileProcessorListener {

    void onLineProcessed(IndexRequest request)

    void onFileProcessed()

    void onAllFileProcessed()

    void onError(Exception e)

    boolean listening()

    void log(String message, Object...args)
}
