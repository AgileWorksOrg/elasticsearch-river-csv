package org.elasticsearch.river.csv

/**
 * Created by vtajzich on 22/02/14.
 */
public interface FileProcessorFactory {

    FileProcessor create(Configuration config, File file, FileProcessorListener listener)
}
