package org.agileworks.elasticsearch.river.csv.listener

import org.agileworks.elasticsearch.river.csv.Configuration
import org.elasticsearch.common.logging.ESLogger
import org.elasticsearch.river.RiverSettings
import spock.lang.Specification

/**
 * Created by vitek on 27/03/14.
 */
class BashFileProcessorListenerSpec extends Specification {

    RiverSettings riverSettings

    ESLogger logger

    BashFileProcessorListener listener


    def setup() {

        Map csvFile = [ 'script_before_all' : getScript('before_import.sh'),
                        'script_after_all' : getScript('after_import.sh'),
                        'script_before_file' : getScript('before_file.sh'),
                        'script_after_file' : getScript('after_file.sh')
        ]

        riverSettings = new RiverSettings(null, ['csv_file': csvFile])

        logger = Mock(ESLogger)

        listener = new BashFileProcessorListener(logger, new Configuration(riverSettings, 'csv'))
    }

    def "test onBeforeProcessingStart"() {

        when:
        listener.onBeforeProcessingStart()

        then:
        1 * logger.info("before all\n")
    }

    def "test onBeforeFileProcess"() {

        when:
        listener.onBeforeFileProcess(new File('test_file.csv'))

        then:
        1 * logger.info("before file test_file.csv\n")
    }

    def "test onFileProcessed"() {

        when:
        listener.onFileProcessed(new File('test_file.csv'))

        then:
        1 * logger.info("after file test_file.csv\n")
    }

    def "test onAllFileProcessed"() {

        when:
        listener.onAllFileProcessed()

        then:
        1 * logger.info("after all\n", [])
    }

    def "test onAllFileProcessed but no script"() {

        given:
        listener.scriptAfterAll = null

        when:
        listener.onAllFileProcessed()

        then:
        0 * logger.info(_)
    }

    String getScript(String name) {
        return getClass().getResource("/shell_scripts/$name").path
    }
}
