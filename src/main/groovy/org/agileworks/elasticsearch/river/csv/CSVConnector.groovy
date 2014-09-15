package org.agileworks.elasticsearch.river.csv

import groovy.transform.CompileStatic
import org.agileworks.elasticsearch.river.csv.listener.FileProcessorListener

class CSVConnector implements Runnable {

    Configuration config
    FileProcessorListener listener
    FileProcessorFactory processorFactory

    CSVConnector(FileProcessorListener listener, Configuration config, FileProcessorFactory processorFactory) {
        this.listener = listener
        this.config = config
        this.processorFactory = processorFactory
    }

    @Override
    public void run() {
        processAllFiles()
    }

    @CompileStatic
    void processAllFiles() {

        while (listener.listening()) {

            File lastProcessedFile = null
            try {

                File[] files = getFiles()

                listener.log('Using configuration: {}', config)
                listener.log('Going to process files {}', files)

                listener.onBeforeProcessingStart(files)

                for (File file : files) {

                    try {

                        listener.log('Processing file {}', file.getName())

                        file = renameFile(file, '.processing')
                        lastProcessedFile = file

                        processorFactory.create(config, file, listener).process()

                        file = renameFile(file, '.imported')
                        lastProcessedFile = file

                    } catch (Exception e) {
                        listener.onErrorAndContinue(e, "Error during processing file '$file.name'. Skipping it.")
                    }
                }

                listener.onAllFileProcessed(files)

            } catch (Exception e) {

                if (lastProcessedFile != null) {
                    renameFile(lastProcessedFile, '.error')
                }

                listener.onError(e)
            }

            if (!listener.listening()) {
                return
            }
        }
    }

    @CompileStatic
    File renameFile(File file, String suffix) {

        File newFile = new File(file.getAbsolutePath() + suffix)

        if (!file.renameTo(newFile)) {
            listener.log('can\'t rename file {} to {}', file.getName(), newFile.getName())
        }

        return newFile
    }

    File[] getFiles() {

        File folder = new File(config.folderName)

        listener.log("All files in folder: " + folder.listFiles().collect { it.name })

        def filter = ['accept': { File file, String s -> s.matches(config.filenamePattern) }] as FilenameFilter

        File[] acceptedFiles = folder.listFiles(filter)

        listener.log("Accepted files: " + acceptedFiles.collect { it.name })

        return acceptedFiles
    }
}
