package org.agileworks.elasticsearch.river.csv

import au.com.bytecode.opencsv.CSVReader
import org.agileworks.elasticsearch.river.csv.listener.FileProcessorListener
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.common.xcontent.XContentFactory

class OpenCSVFileProcessor implements FileProcessor {

    final Configuration config
    final File file
    final FileProcessorListener listener

    OpenCSVFileProcessor(Configuration config, File file, FileProcessorListener listener) {
        this.config = config
        this.file = file
        this.listener = listener
    }

    void process() {

        listener.onBeforeFileProcess(file)

        long linesCount = 0

        CSVReader reader = new CSVReader(new FileReader(file), config.separator.charValue(), config.quoteCharacter, config.escapeCharacter)

        String[] nextLine

        while ((nextLine = reader.readNext()) != null) {

            if (linesCount == 0 && config.firstLineIsHeader) {

                config.csvFields = Arrays.asList(nextLine)

            } else if (nextLine.length > 0 && !(nextLine.length == 1 && nextLine[0].trim().equals(''))) {

                try {
                    processDataLine(nextLine)
                } catch (Exception e) {
                    listener.onErrorAndContinue(e, "Error has occured during processing file '$file.name' , skipping line: '${nextLine}' and continue in processing")
                }
            }

            linesCount++
        }

        listener.onFileProcessed(file)

        listener.log("File ${file.getName()}, processed lines $linesCount")
    }

    private void processDataLine(String[] line) {

        XContentBuilder builder = XContentFactory.jsonBuilder()
        builder.startObject()

        int position = 0
        for (Object fieldName : config.csvFields) {

            if (fieldName != config.idField) {
                builder.field((String) fieldName, line[position])
            }

            position++
        }

        builder.endObject()

        IndexRequest request = Requests.indexRequest(config.indexName).type(config.typeName)

        if (csvContainsIDColumn()) {
            request.id(getId(line))
        } else {
            request.id(UUID.randomUUID().toString())
        }

        request.create(false).source(builder)

        listener.onLineProcessed(request)
    }

    boolean csvContainsIDColumn() {
        return config.csvFields.find { it == config.idField }
    }

    String getId(String[] line) {

        int index = config.csvFields.indexOf(config.idField)

        return line[index]
    }
}
