package org.agileworks.elasticsearch.river.csv.listener

import org.agileworks.elasticsearch.river.csv.Configuration
import org.agileworks.elasticsearch.river.csv.processrunner.ProcessRunnerFactory
import org.agileworks.elasticsearch.river.csv.processrunner.ProcessRunner
import org.elasticsearch.common.logging.ESLogger
import org.elasticsearch.river.RiverSettings
import spock.lang.Specification

/**
 * Created by vitek on 27/03/14.
 */
class BashFileProcessorListenerTest extends Specification {

    RiverSettings riverSettings

    ESLogger logger

    BashFileProcessorListener listener


    def setup() {

        Map csvFile

        ProcessRunner processRunner = ProcessRunnerFactory.instance.getRunner()

        if (System.getProperty("os.name").toLowerCase().contains("windows")) {

            csvFile = [ 'script_before_all' : getScript('before_import.bat'),
                            'script_after_all' : getScript('after_import.bat'),
                            'script_before_file' : getScript('before_file.bat'),
                            'script_after_file' : getScript('after_file.bat')
            ]
        }
        else {

            csvFile = [ 'script_before_all' : getScript('before_import.sh'),
                            'script_after_all' : getScript('after_import.sh'),
                            'script_before_file' : getScript('before_file.sh'),
                            'script_after_file' : getScript('after_file.sh')
            ]
            
            csvFile.values().each {
                "chmod +x $it".execute()
            }
        }

        riverSettings = new RiverSettings(null, ['csv_file': csvFile])

        logger = Mock(ESLogger)

        listener = new BashFileProcessorListener(logger, new Configuration(riverSettings, 'csv'), processRunner)
    }

    def "test onBeforeProcessingStart"() {

        when:
        File[] files = [new File('test_file.csv')]
        listener.onBeforeProcessingStart(files)

        then:
        1 * logger.info(String.format("before all%n"))
    }

    def "test onBeforeFileProcess"() {

        when:
        listener.onBeforeFileProcess(new File('test_file.csv'))

        then:
        1 * logger.info(String.format("before file test_file.csv%n"))
    }

    def "test onFileProcessed"() {

        when:
        listener.onFileProcessed(new File('test_file.csv'))

        then:
        1 * logger.info(String.format("after file test_file.csv%n"))
    }

    def "test onAllFileProcessed"() {

        when:
        File[] files = [new File('test_file.csv')]
        listener.onAllFileProcessed(files)

        then:
        1 * logger.info(String.format("after all%n"), [])
    }

    def "test onAllFileProcessed but no script"() {

        given:
        listener.scriptAfterAll = null

        when:
        File[] files = [new File('test_file.csv')]
        listener.onAllFileProcessed(files)

        then:
        0 * logger.info(_)
    }

    String getScript(String name) {
        return getClass().getResource("/shell_scripts/$name").path
    }
}
