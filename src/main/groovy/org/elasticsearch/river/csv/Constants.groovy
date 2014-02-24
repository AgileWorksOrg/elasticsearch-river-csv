package org.elasticsearch.river.csv

class Constants {

    static final String CSV_FILE = 'csv_file'
    static final String INDEX = 'index'

    static class CSV {

        static final String CSV_FILE_IS_HEADER = 'first_line_is_header'
        static final String FOLDER = 'folder'

        static final String FILENAME_PATTERN = 'filename_pattern'
        static final String FILENAME_PATTERN_VALUE = '.*\\.csv$'

        static final String FIELDS = 'fields'
        static final String POLL = 'poll'

        static final String ESCAPE_CHARACTER = 'escape_character'
        static final String ESCAPE_CHARACTER_VALUE = '\\'


        static final String FIELD_SEPARATOR = 'field_separator'
        static final String QUOTE_CHARACTER = 'quote_character'
        static final String FIELD_ID = 'field_id'
    }

    static class Index {

        static final String TYPE = 'type'
        static final String TYPE_VALUE = 'csv_type'

        static final String BULK_SIZE = 'bulk_size'
        static final String BULK_THRESHOLD = 'bulk_threshold'
    }
}
