package org.agileworks.elasticsearch.river.csv.listener

import org.agileworks.elasticsearch.river.csv.Configuration
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.common.logging.ESLogger

/**
 * Created by vitek on 27/03/14.
 */
class BashFileProcessorListener implements FileProcessorListener {

    ESLogger logger

    File scriptBeforeAll
    File scriptAfterAll
    File scriptBeforeFile
    File scriptAfterFile

    BashFileProcessorListener(ESLogger logger, Configuration configuration) {
        this.logger = logger

        if (configuration.scriptBeforeAll) {
            scriptBeforeAll = new File(configuration.scriptBeforeAll)
        }

        if (configuration.scriptAfterAll) {
            scriptAfterAll = new File(configuration.scriptAfterAll)
        }

        if (configuration.scriptBeforeFile) {
            scriptBeforeFile = new File(configuration.scriptBeforeFile)
        }

        if (configuration.scriptAfterFile) {
            scriptAfterFile = new File(configuration.scriptAfterFile)
        }
    }

    @Override
    void onBeforeProcessingStart(File[] files) {
        runAndLog(scriptBeforeAll, files)
    }

    @Override
    void onBeforeFileProcess(File file) {
        runAndLog(scriptBeforeFile, file)
    }

    @Override
    void onLineProcessed(IndexRequest request) {

    }

    @Override
    void onFileProcessed(File file) {
        runAndLog(scriptAfterFile, file)
    }

    @Override
    void onAllFileProcessed(File[] files) {
        runAndLog(scriptAfterAll, files)
    }

    @Override
    void onError(Exception e) {

    }

    @Override
    void onErrorAndContinue(Exception e, String message) {

    }

    @Override
    boolean listening() {
        return false
    }

    @Override
    void log(String message, Object... args) {

    }

    @Override
    void log(String message) {

    }

    void runAndLog(File file, Object... args) {

        if(file && file.exists()) {

            String result = runScript(file, args)
            logger.info(result)
        }
    }

    String runScript(File file, Object... args) {

        try {

            if (file && file.exists()) {
                def process = [file.absolutePath, args.join(" ")].execute()
                return process.text
            }

        } catch (Exception e) {
            logger.error("Cannot run script $file.absolutePath", e)
            return ''
        }
    }
}
