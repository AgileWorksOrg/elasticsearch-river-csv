package org.agileworks.elasticsearch.river.csv

import groovy.transform.CompileStatic

/**
 * Purpose to have a factory in this small project is to provide possibility
 * to unit test other parts
 */
@CompileStatic
class OpenCSVFileProcessorFactory implements FileProcessorFactory {

    @Override
    FileProcessor create(Configuration config, File file, FileProcessorListener listener) {
        return new OpenCSVFileProcessor(config, file, listener)
    }
}
