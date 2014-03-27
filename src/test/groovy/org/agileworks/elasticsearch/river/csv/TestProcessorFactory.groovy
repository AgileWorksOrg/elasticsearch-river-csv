package org.agileworks.elasticsearch.river.csv

import org.agileworks.elasticsearch.river.csv.listener.FileProcessorListener

/**
 * Created by vtajzich on 22/02/14.
 */
class TestProcessorFactory implements FileProcessorFactory {

    FileProcessor fileProcessor

    @Override
    FileProcessor create(Configuration config, File file, FileProcessorListener listener) {
        return fileProcessor
    }
}
