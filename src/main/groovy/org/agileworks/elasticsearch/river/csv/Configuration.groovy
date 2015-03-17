package org.agileworks.elasticsearch.river.csv

import groovy.transform.CompileStatic
import groovy.transform.ToString
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.river.RiverSettings

import java.nio.charset.Charset

import static org.elasticsearch.common.xcontent.support.XContentMapValues.*

@ToString
@CompileStatic
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
    int bulkThreshold
    int concurrentRequests
    String idField
    boolean idFieldInclude = false
    String timestampField

    String scriptBeforeAll
    String scriptAfterAll
    String scriptBeforeFile
    String scriptAfterFile

    Charset charset = Charset.forName('UTF-8')

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
            idFieldInclude = nodeBooleanValue(csvSettings.get(Constants.CSV.FIELD_ID_INCLUDE, false))
            timestampField = nodeStringValue(csvSettings.get(Constants.CSV.FIELD_TIMESTAMP), null)
            concurrentRequests = nodeIntegerValue(csvSettings.get(Constants.CSV.CONCURRENT_REQUESTS), 1)

            String charsetName = nodeStringValue(csvSettings.get(Constants.CSV.CHARSET), 'UTF-8')

            try {

                charset = Charset.forName(charsetName)

            } catch (Exception e) {
                throw new ConfigurationException("""Charset name "$charsetName" is not valid.
Consider to use one of:

Charset     Description
=====================================

US-ASCII    Seven-bit ASCII, a.k.a. ISO646-US, a.k.a. the Basic Latin block of the Unicode character set
ISO-8859-1  ISO Latin Alphabet No. 1, a.k.a. ISO-LATIN-1
UTF-8       Eight-bit UCS Transformation Format
UTF-16BE    Sixteen-bit UCS Transformation Format, big-endian byte order
UTF-16LE    Sixteen-bit UCS Transformation Format, little-endian byte order
UTF-16      Sixteen-bit UCS Transformation Format, byte order identified by an optional byte-order mark

More details about charsets are available at http://docs.oracle.com/javase/6/docs/api/java/nio/charset/Charset.html
""")
            }

            scriptBeforeAll = nodeStringValue(csvSettings.get(Constants.CSV.SCRIPT_BEFORE_ALL), null)
            scriptAfterAll = nodeStringValue(csvSettings.get(Constants.CSV.SCRIPT_AFTER_ALL), null)
            scriptBeforeFile = nodeStringValue(csvSettings.get(Constants.CSV.SCRIPT_BEFORE_FILE), null)
            scriptAfterFile = nodeStringValue(csvSettings.get(Constants.CSV.SCRIPT_AFTER_FILE), null)

        } else {
            throw new ConfigurationException("""No csv_file configuration found. See read.me (https://github.com/xxBedy/elasticsearch-river-csv). Did you rename "csv_file" to something custom? """)
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
