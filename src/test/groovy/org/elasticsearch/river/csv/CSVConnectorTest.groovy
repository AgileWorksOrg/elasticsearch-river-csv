package org.elasticsearch.river.csv

import org.elasticsearch.river.RiverSettings
import spock.lang.Specification

class CSVConnectorTest extends Specification {

    FileProcessorFactory factory

    FileProcessorListener listener

    FileProcessor processor

    Configuration configuration

    CSVConnector connector

    void setup() {

        File testTempDir = copyTestFilesToTemp()

        configuration = new Configuration(new RiverSettings(null, ['csv_file': ['folder': testTempDir.absolutePath]]), 'myRiver')
        listener = GroovyMock(FileProcessorListener)
        processor = GroovyMock(FileProcessor)

        factory = new TestProcessorFactory(fileProcessor: processor)

        connector = new CSVConnector(listener, configuration, factory)
    }

    def "process all files once"() {

        when:

        connector.processAllFiles()

        then:

        listener.listening() >> { connector.files.length > 0 }

        5 * processor.process()

        1 * listener.onAllFileProcessed()

        _ * listener.log(_, _)

        _ * listener.toString()

    }

    def "get files for processing"() {

        when:

        File[] files = connector.files

        then:

        files.length == 5
    }

    File getTestSourceFolder() {

        URL url = getClass().getResource("/test_1.csv")
        new File(url.path).parentFile
    }

    File copyTestFilesToTemp() {

        File tempDir = new File(System.properties.'java.io.tmpdir')

        File testTempDir = new File(tempDir, 'CSVConnectorTest')

        testTempDir.mkdirs()
        testTempDir.listFiles()*.delete()

        getTestSourceFolder().listFiles().findAll { it.isFile() }.each {
            new File(testTempDir, it.name) << it.text
        }

        return testTempDir
    }
}
