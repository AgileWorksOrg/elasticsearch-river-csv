package org.agileworks.elasticsearch.river.csv

import groovy.transform.ToString
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.river.RiverSettings

import java.util.concurrent.atomic.AtomicInteger

import static org.elasticsearch.common.xcontent.support.XContentMapValues.*

@ToString
class Configuration {

    String folderName
    String filenamePattern

    boolean firstLineIsHeader = false

    List<Object> csvFields
    TimeValue poll

    String indexName
    String typeName

    int bulkSize

    char escapeCharacter
    char quoteCharacter
    char separator
    AtomicInteger onGoingBulks = new AtomicInteger()
    int bulkThreshold
    String idField

    Configuration(RiverSettings settings, String riverName) {

        if (settings.settings().containsKey(Constants.CSV_FILE)) {

            Map<String, Object> csvSettings = (Map<String, Object>) settings.settings().get(Constants.CSV_FILE)

            firstLineIsHeader = nodeBooleanValue(csvSettings.get(Constants.CSV.CSV_FILE_IS_HEADER, false))
            folderName = nodeStringValue(csvSettings.get(Constants.CSV.FOLDER), null)
            filenamePattern = nodeStringValue(csvSettings.get(Constants.CSV.FILENAME_PATTERN), Constants.CSV.FILENAME_PATTERN_VALUE)
            csvFields = extractRawValues(Constants.CSV.FIELDS, csvSettings)
            poll = nodeTimeValue(csvSettings.get(Constants.CSV.POLL), TimeValue.timeValueMinutes(60))
            escapeCharacter = nodeStringValue(csvSettings.get(Constants.CSV.ESCAPE_CHARACTER), String.valueOf(Constants.CSV.ESCAPE_CHARACTER_VALUE)).charAt(0)
            separator = nodeStringValue(csvSettings.get(Constants.CSV.FIELD_SEPARATOR), String.valueOf(',')).charAt(0)
            quoteCharacter = nodeStringValue(csvSettings.get(Constants.CSV.QUOTE_CHARACTER), String.valueOf('\"')).charAt(0)
            idField = nodeStringValue(csvSettings.get(Constants.CSV.FIELD_ID), 'id')

        } else {
            throw new ConfigurationException("No csv_file configuration found. See read.me (https://github.com/xxBedy/elasticsearch-river-csv)")
        }

        if (settings.settings().containsKey(Constants.INDEX)) {

            Map<String, Object> indexSettings = (Map<String, Object>) settings.settings().get(Constants.INDEX)

            indexName = nodeStringValue(indexSettings.get(Constants.INDEX), riverName)
            typeName = nodeStringValue(indexSettings.get(Constants.Index.TYPE), Constants.Index.TYPE_VALUE)
            bulkSize = nodeIntegerValue(indexSettings.get(Constants.Index.BULK_SIZE), 100)
            bulkThreshold = nodeIntegerValue(indexSettings.get(Constants.Index.BULK_THRESHOLD), 10)

        } else {
            indexName = riverName
            typeName = Constants.Index.TYPE_VALUE
            bulkSize = 100
            bulkThreshold = 10
        }
    }
}
