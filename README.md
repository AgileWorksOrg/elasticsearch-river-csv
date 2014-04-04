CSV River Plugin for ElasticSearch
==================================

The CSV River plugin allows index CSV files in folder.

In order to install the plugin, simply run: 
`bin/plugin -install river-csv -url https://github.com/xxBedy/elasticsearch-river-csv/releases/download/2.0.1/elasticsearch-river-csv-2.0.1.zip`.

If it doesn't work, clone git repository and build plugin manually.


    -------------------------------------
    | CSV Plugin | ElasticSearch        |
    -------------------------------------
    | master         | 1.0.x -> master |
    -------------------------------------
    | 2.0.1          | 1.0.x -> master  |
    -------------------------------------
    | 2.0.0          | 1.0.0            |
    -------------------------------------
    | 1.0.1          | 0.19.x           |
    -------------------------------------
    | 1.0.0          | 0.19.x           |
    -------------------------------------

The CSV river import data from CSV files and index it.

##Changelog

###2.0.1

* Replaced custom upload logic with BulkProcessor. Inspired from pullrequest by https://github.com/aritratony . Thanks!
* Added error handling - if something went wrong during line or file processing it is logged, skipped and import continues
* Added callback feature - custom shell scripts can be called on events: before import, before file processing start, after file is processed and after all files are processed

###2.0.0

* Updated to ES 1.0.0
* Fixed - when input file was smaller than batch it didn't saved to ES
* Added - id column name, documents with ID column can be updated
* Added - ability to parse first row as header, must be enabled by property


##Creating the CSV river can be done using:


###Minimal curl

	curl -XPUT localhost:9200/_river/my_csv_river/_meta -d '
    {
        "type" : "csv",
        "csv_file" : {
            "folder" : "/tmp",
            "first_line_is_header":"true"
        }
    }'

###Full request

    curl -XPUT localhost:9200/_river/my_csv_river/_meta -d '
	{
	    "type" : "csv",
	    "csv_file" : {
	        "folder" : "/tmp",
	        "filename_pattern" : ".*\\.csv$",
	        "poll":"5m",
	        "fields" : [
	            "column1",
	            "column2",
	            "column3",
	            "column4"
	        ],
            "first_line_is_header" : "false",
	        "field_separator" : ",",
	        "escape_character" : ";",
	        "quote_character" : "'",
            "field_id" : "id",
            "concurrent_requests" : "1",
            "script_before_all": "/path/to/before_all.sh",
            "script_after_all": "/path/to/after_all.sh",
            "script_before_file": "/path/to/before_file.sh",
            "script_after_file": "/path/to/after_file.sh"
	    },
	    "index" : {
	        "index" : "my_csv_data",
	        "type" : "csv_type",
	        "bulk_size" : 100,
	        "bulk_threshold" : 10
	    }
	}'

* takes no arguments

	    "script_before_all": "/path/to/before_all.sh"
    	"script_after_all": "/path/to/after_all.sh"

* file path is argument

        "script_before_file": "/path/to/before_file.sh",
        "script_after_file": "/path/to/after_file.sh"

####Examples how the files look like

        #!/bin/sh
        echo "greetings from shell before all"


        #!/bin/bash
        echo "greetings from shell before file $1"


        #!/bin/bash
        echo "greetings from shell after file $1"


        #!/bin/bash
        echo "greetings from shell after all"


###Optional parameters:

fields = empty - MUST BE SET or first_line_is_header must be set to true

    ----------------------------------------
    | Name              | Default value    |
    ----------------------------------------
    | first_line_is_header   | false       |
    ----------------------------------------
    | filename_pattern   | .*\\.csv$       |
    ----------------------------------------
    | poll              |   60 minutes     |
    ----------------------------------------
    | field_separator   | ,                |
    ----------------------------------------
    | escape_character  | \                |
    ----------------------------------------
    | quote_character   | "                |
    ----------------------------------------
    | bulk_size         | 100              |
    ----------------------------------------
    | bulk_threshold    | 10               |
    ----------------------------------------
    | concurrent_requests | 1              |
    ----------------------------------------


License
-------

    This software is licensed under the Apache 2 license, quoted below.

    Copyright 2012-2013 Martin Bednar, Vitek Tajzich

    Licensed under the Apache License, Version 2.0 (the "License"); you may not
    use this file except in compliance with the License. You may obtain a copy of
    the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
    License for the specific language governing permissions and limitations under
    the License.
