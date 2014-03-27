package org.agileworks.elasticsearch.river.csv

import org.elasticsearch.action.index.IndexRequest

/**
 * It's being called from FileProcessor and CSVConnector in order
 * to notify river what's happening
 */
public interface FileProcessorListener {

    void onBeforeProcessingStart()

    void onLineProcessed(IndexRequest request)

    void onFileProcessed(File file)

    void onAllFileProcessed()

    void onError(Exception e)

    void onErrorAndContinue(Exception e, String message)

    boolean listening()

    void log(String message, Object...args)
}
