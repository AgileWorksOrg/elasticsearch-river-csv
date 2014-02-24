package org.elasticsearch.river.csv

import groovy.transform.CompileStatic

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

                for (File file : files) {

                    listener.log('Processing file {}', file.getName())

                    file = renameFile(file, '.processing')
                    lastProcessedFile = file

                    processorFactory.create(config, file, listener).process()

                    file = renameFile(file, '.imported')
                    lastProcessedFile = file
                }

                listener.onAllFileProcessed()

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

        def filter = ['accept': {File file, String s -> s.matches(config.filenamePattern)}] as FilenameFilter

        return folder.listFiles(filter)
    }
}
