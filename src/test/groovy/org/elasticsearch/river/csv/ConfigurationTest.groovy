package org.elasticsearch.river.csv

import org.elasticsearch.river.RiverSettings
import spock.lang.Specification

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
        config.indexName
        config.poll
        config.quoteCharacter
        config.separator
        config.typeName
    }
}
