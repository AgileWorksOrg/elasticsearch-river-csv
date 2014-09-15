package org.agileworks.elasticsearch.river.csv.listener

import groovy.transform.CompileStatic
import org.elasticsearch.action.index.IndexRequest

/**
 * Created by vitek on 27/03/14.
 */
@CompileStatic
class DelegatingFileProcessorListener implements FileProcessorListener {

    FileProcessorListener[] listeners = []

    DelegatingFileProcessorListener(FileProcessorListener...listeners) {
        this.listeners = listeners
    }

    @Override
    void onBeforeProcessingStart(File[] files) {
        listeners*.onBeforeProcessingStart(files)
    }

    @Override
    void onBeforeFileProcess(File file) {
        listeners*.onBeforeFileProcess(file)
    }

    @Override
    void onLineProcessed(IndexRequest request) {
        listeners*.onLineProcessed(request)
    }

    @Override
    void onFileProcessed(File file) {
        listeners*.onFileProcessed(file)
    }

    @Override
    void onAllFileProcessed(File[] files) {
        listeners*.onAllFileProcessed(files)
    }

    @Override
    void onError(Exception e) {
        listeners*.onError(e)
    }

    @Override
    void onErrorAndContinue(Exception e, String message) {
        listeners*.onErrorAndContinue(e, message)
    }

    @Override
    boolean listening() {
        return listeners.find { FileProcessorListener listener -> listener.listening() } != null
    }

    @Override
    void log(String message, Object... args) {
        listeners*.log(message, args)
    }

    @Override
    void log(String message) {
        listeners*.log(message)
    }
}
