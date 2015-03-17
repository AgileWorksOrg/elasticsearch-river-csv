package org.agileworks.elasticsearch.river.csv

import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.common.joda.time.DateTimeZone
import org.elasticsearch.common.joda.time.format.DateTimeFormatter
import org.elasticsearch.common.joda.time.format.ISODateTimeFormat
import org.elasticsearch.river.RiverSettings
import spock.lang.Specification

import java.nio.charset.Charset

class OpenCSVFileProcessorTest extends Specification {


    public static final ArrayList<String> DEFAULT_BODIES = ['{"Year":"1997","Make":"Ford","Model":"E350"}', '{"Year":"2000","Make":"Mercury","Model":"Cougar"}']
    public static final ArrayList<String> TURKISH_BODIES = ['{"Title":"Decortie Labirent Kitaplık - Venge","":""}', '{"Title":"Erciyes Dağı - Kapadokya Tablosu RMB-237 - 190x120 cm","":""}']

    OpenCSVFileProcessor processor

    Configuration configuration

    TestFileListener listener

    void setup() {

        listener = new TestFileListener()

        configuration = new Configuration(new RiverSettings(null, ['csv_file': [:]]), 'myRiver')
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

        requests[0].source().toUtf8() == DEFAULT_BODIES[0]
        requests[1].source().toUtf8() == DEFAULT_BODIES[1]
    }

    def "process file w/ timestamp"() {

        given:

        configuration.firstLineIsHeader = true
        configuration.timestampField = "UpdatedAt"

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

        DateTimeFormatter timestampFormatter = ISODateTimeFormat.dateTime().withZone(DateTimeZone.UTC)
        String timestamp = timestampFormatter.print(processor.getStartTime().getTime())
        requests[0].source().toUtf8() == '{"Year":"1997","Make":"Ford","Model":"E350","UpdatedAt":"' + timestamp + '"}'
    }

    def "process file w/ header and id column"() {

        given:

        configuration.firstLineIsHeader = true
        configuration.idField = idColumnName
        configuration.idFieldInclude = idColumnInclude
        configuration.charset = Charset.forName(charset)
        configuration.separator = separator

        processor = new OpenCSVFileProcessor(configuration, getTestCsv("${fileName}.csv"), listener)

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

        requests[0].id() == idValue[0]
        requests[1].id() == idValue[1]

        requests[0].source().toUtf8() == body[0]
        requests[1].source().toUtf8() == body[1]

        where:

        idColumnName    | idColumnInclude | fileName                         | idValue            | charset    | separator | body
        'id'            | false           | 'test_1_id_column'               | ['1', '2']         | 'UTF-8'    | ','       | DEFAULT_BODIES
        'ProductNumber' | false           | 'test_1_ProductNumber_id_column' | ['223', '229']     | 'UTF-8'    | ','       | DEFAULT_BODIES
        'ProductNumber' | false           | 'turkish_encoding'               | ['69377', '69379'] | 'UTF-16LE' | ';'       | TURKISH_BODIES
        'id'            | true            | 'test_1_id_column'               | ['1', '2']         | 'UTF-8'    | ','       | ['{"id":"1","Year":"1997","Make":"Ford","Model":"E350"}', '{"id":"2","Year":"2000","Make":"Mercury","Model":"Cougar"}']
        'ProductNumber' | true            | 'test_1_ProductNumber_id_column' | ['223', '229']     | 'UTF-8'    | ','       | ['{"ProductNumber":"223","Year":"1997","Make":"Ford","Model":"E350"}', '{"ProductNumber":"229","Year":"2000","Make":"Mercury","Model":"Cougar"}']
        'ProductNumber' | true            | 'turkish_encoding'               | ['69377', '69379'] | 'UTF-16LE' | ';'       | ['{"ProductNumber":"69377","Title":"Decortie Labirent Kitaplık - Venge","":""}', '{"ProductNumber":"69379","Title":"Erciyes Dağı - Kapadokya Tablosu RMB-237 - 190x120 cm","":""}']
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

        requests[0].source().toUtf8() == DEFAULT_BODIES[0]
        requests[1].source().toUtf8() == DEFAULT_BODIES[1]
    }

    def "process file w/o header and tab separator"() {

        given:

        configuration.csvFields = ['Year', 'Make', 'Model']
        configuration.separator = '\t'
        processor = new OpenCSVFileProcessor(configuration, getTestCsv('test_1_tab_separator.csv'), listener)

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

        requests[0].source().toUtf8() == DEFAULT_BODIES[0]
        requests[1].source().toUtf8() == DEFAULT_BODIES[1]
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

    def "process file w/ header and one line has wrong number of columns"() {

        given:

        configuration.firstLineIsHeader = true

        processor = new OpenCSVFileProcessor(configuration, getTestCsv('test_1_wrong_columns_count.csv'), listener)

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

        requests[0].source().toUtf8() == DEFAULT_BODIES[0]
        requests[1].source().toUtf8() == DEFAULT_BODIES[1]
    }

    File getTestCsv(String name) {

        URL url = getClass().getResource("/$name")

        new File(url.path)
    }
}
