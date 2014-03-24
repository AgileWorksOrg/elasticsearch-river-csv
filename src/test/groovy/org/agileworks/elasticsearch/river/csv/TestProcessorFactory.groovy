package org.agileworks.elasticsearch.river.csv

import org.agileworks.elasticsearch.river.csv.Configuration
import org.agileworks.elasticsearch.river.csv.FileProcessor
import org.agileworks.elasticsearch.river.csv.FileProcessorFactory
import org.agileworks.elasticsearch.river.csv.FileProcessorListener

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
