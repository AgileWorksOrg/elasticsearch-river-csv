package org.elasticsearch.river.csv

import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.river.RiverSettings
import spock.lang.Specification

class OpenCSVFileProcessorTest extends Specification {

    OpenCSVFileProcessor processor

    Configuration configuration

    TestFileListener listener

    void setup() {

        listener = new TestFileListener()

        configuration = new Configuration(new RiverSettings(null, ['csv_file':[:]]), 'myRiver')
        configuration.indexName = 'myIndex'
        configuration.typeName = 'csv'
    }

    def "process file w/ header"() {

        given:

        configuration.firstLineIsHeader = true

        processor = new OpenCSVFileProcessor(configuration, getTestCsv('test_1.csv'), listener)

        when:
        processor.process()

        then:

        listener.fileProcessed
        listener.requests.size() == 2

        List<IndexRequest> requests = listener.requests

        requests.each { IndexRequest request ->
            IndexRequest.OpType.INDEX == request.opType()
            configuration.indexName == request.index()
            configuration.typeName == request.type()
        }

        requests[0].source().toUtf8() == '{"Year":"1997","Make":"Ford","Model":"E350"}'
        requests[1].source().toUtf8() == '{"Year":"2000","Make":"Mercury","Model":"Cougar"}'
    }

    def "process file w/ header and id column"() {

        given:

        configuration.firstLineIsHeader = true

        processor = new OpenCSVFileProcessor(configuration, getTestCsv('test_1_id_column.csv'), listener)

        when:
        processor.process()

        then:

        listener.fileProcessed
        listener.requests.size() == 2

        List<IndexRequest> requests = listener.requests

        requests.each { IndexRequest request ->
            IndexRequest.OpType.INDEX == request.opType()
            configuration.indexName == request.index()
            configuration.typeName == request.type()
        }

        requests[0].id() == '1'
        requests[1].id() == '2'

        requests[0].source().toUtf8() == '{"Year":"1997","Make":"Ford","Model":"E350"}'
        requests[1].source().toUtf8() == '{"Year":"2000","Make":"Mercury","Model":"Cougar"}'
    }

    def "process file w/o header"() {

        given:

        configuration.csvFields = ['Year', 'Make', 'Model']
        processor = new OpenCSVFileProcessor(configuration, getTestCsv('test_1_no_header.csv'), listener)

        when:
        processor.process()

        then:

        listener.fileProcessed
        listener.requests.size() == 2

        List<IndexRequest> requests = listener.requests

        requests.each { IndexRequest request ->
            IndexRequest.OpType.INDEX == request.opType()
            configuration.indexName == request.index()
            configuration.typeName == request.type()
        }

        requests[0].source().toUtf8() == '{"Year":"1997","Make":"Ford","Model":"E350"}'
        requests[1].source().toUtf8() == '{"Year":"2000","Make":"Mercury","Model":"Cougar"}'
    }

    def "process quoted file w/ header"() {

        given:

        configuration.firstLineIsHeader = true

        processor = new OpenCSVFileProcessor(configuration, getTestCsv('test_2.csv'), listener)

        when:
        processor.process()

        then:

        listener.fileProcessed
        listener.requests.size() == 4

        List<IndexRequest> requests = listener.requests

        requests.each { IndexRequest request ->
            request.id()
            IndexRequest.OpType.INDEX == request.opType()
            configuration.indexName == request.index()
            configuration.typeName == request.type()
        }

        requests[0].source().toUtf8() == '{"Year":"1997","Make":"Ford","Model":"E350","Description":"ac, abs, moon","Price":"3000.00"}'
        requests[1].source().toUtf8() == '{"Year":"1999","Make":"Chevy","Model":"Venture \\"Extended Edition\\"","Description":"","Price":"4900.00"}'
        requests[2].source().toUtf8() == '{"Year":"1999","Make":"Chevy","Model":"Venture \\"Extended Edition, Very Large\\"","Description":"","Price":"5000.00"}'
        requests[3].source().toUtf8() == '{"Year":"1996","Make":"Jeep","Model":"Grand Cherokee","Description":"MUST SELL!air, moon roof, loaded","Price":"4799.00"}'
    }

    def "process file w/ header and decimal"() {

        given:

        configuration.firstLineIsHeader = true

        processor = new OpenCSVFileProcessor(configuration, getTestCsv('test_3_decimal.csv'), listener)

        when:
        processor.process()

        then:

        listener.fileProcessed
        listener.requests.size() == 2

        List<IndexRequest> requests = listener.requests

        requests.each { IndexRequest request ->
            IndexRequest.OpType.INDEX == request.opType()
            configuration.indexName == request.index()
            configuration.typeName == request.type()
        }

        requests[0].source().toUtf8() == '{"Year":"1997","Make":"Ford","Model":"E350","Length":"2.34"}'
        requests[1].source().toUtf8() == '{"Year":"2000","Make":"Mercury","Model":"Cougar","Length":"2.38"}'
    }

    File getTestCsv(String name) {

        URL url = getClass().getResource("/$name")

        new File(url.path)
    }
}
