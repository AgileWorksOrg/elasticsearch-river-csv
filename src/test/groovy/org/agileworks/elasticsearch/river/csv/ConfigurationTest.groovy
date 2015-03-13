package org.agileworks.elasticsearch.river.csv

import org.elasticsearch.river.RiverSettings
import spock.lang.Specification

import java.nio.charset.Charset

class ConfigurationTest extends Specification {

    static String riverName = 'myRiver'

    RiverSettings riverSettings

    Configuration config


    void setup() {

    }

    void 'missing csv_file config'() {

        given:

        riverSettings = new RiverSettings(null, [:])

        when:
        config = new Configuration(riverSettings, riverName)

        then:

        thrown(ConfigurationException)
    }

    void 'wrong charset'() {

        given:

        riverSettings = new RiverSettings(null, ['csv_file': ['charset':'NON_EXISTING']])

        when:
        config = new Configuration(riverSettings, riverName)

        then:

        thrown(ConfigurationException)
    }

    void 'correct charset'() {

        given:

        riverSettings = new RiverSettings(null, ['csv_file': ['charset':'UTF-16LE']])

        when:
        config = new Configuration(riverSettings, riverName)

        then:

        config.charset == Charset.forName('UTF-16LE')
    }

    void 'empty csv_file config'() {

        given:

        riverSettings = new RiverSettings(null, ['csv_file': [:]])

        when:
        config = new Configuration(riverSettings, riverName)

        then:

        notThrown(ConfigurationException)

        !config.firstLineIsHeader
        !config.csvFields
        config.escapeCharacter
        config.filenamePattern
        !config.folderName
        config.idField
        !config.idFieldInclude
        config.indexName
        config.poll
        config.quoteCharacter
        config.separator
        config.typeName
    }
}
