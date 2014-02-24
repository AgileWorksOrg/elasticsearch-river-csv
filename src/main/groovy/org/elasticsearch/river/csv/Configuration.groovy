package org.elasticsearch.river.csv

import groovy.transform.ToString
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.river.RiverSettings

import java.util.concurrent.atomic.AtomicInteger

import static org.elasticsearch.common.xcontent.support.XContentMapValues.*
import static org.elasticsearch.river.csv.Constants.CSV.*
import static org.elasticsearch.river.csv.Constants.*
import static org.elasticsearch.river.csv.Constants.Index.*

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

        if (settings.settings().containsKey(CSV_FILE)) {

            Map<String, Object> csvSettings = (Map<String, Object>) settings.settings().get(CSV_FILE)

            firstLineIsHeader = nodeBooleanValue(csvSettings.get(CSV_FILE_IS_HEADER, false))
            folderName = nodeStringValue(csvSettings.get(FOLDER), null)
            filenamePattern = nodeStringValue(csvSettings.get(FILENAME_PATTERN), FILENAME_PATTERN_VALUE)
            csvFields = extractRawValues(FIELDS, csvSettings)
            poll = nodeTimeValue(csvSettings.get(POLL), TimeValue.timeValueMinutes(60))
            escapeCharacter = nodeStringValue(csvSettings.get(ESCAPE_CHARACTER), String.valueOf(ESCAPE_CHARACTER_VALUE)).charAt(0)
            separator = nodeStringValue(csvSettings.get(FIELD_SEPARATOR), String.valueOf(',')).charAt(0)
            quoteCharacter = nodeStringValue(csvSettings.get(QUOTE_CHARACTER), String.valueOf('\"')).charAt(0)
            idField = nodeStringValue(csvSettings.get(FIELD_ID), 'id')

        } else {
            throw new ConfigurationException("No csv_file configuration found. See read.me (https://github.com/xxBedy/elasticsearch-river-csv)")
        }

        if (settings.settings().containsKey(INDEX)) {

            Map<String, Object> indexSettings = (Map<String, Object>) settings.settings().get(INDEX)

            indexName = nodeStringValue(indexSettings.get(INDEX), riverName)
            typeName = nodeStringValue(indexSettings.get(TYPE), TYPE_VALUE)
            bulkSize = nodeIntegerValue(indexSettings.get(BULK_SIZE), 100)
            bulkThreshold = nodeIntegerValue(indexSettings.get(BULK_THRESHOLD), 10)

        } else {
            indexName = riverName
            typeName = TYPE_VALUE
            bulkSize = 100
            bulkThreshold = 10
        }
    }
}
