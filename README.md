CSV River Plugin for ElasticSearch
==================================

The CSV River plugin allows index CSV files in folder.

In order to install the plugin, simply run: 
```bin/plugin -install river-csv -url https://github.com/AgileWorksOrg/elasticsearch-river-csv/releases/download/2.2.0/elasticsearch-river-csv-2.2.0.zip```.

If it doesn't work, clone git repository and build plugin manually.


    -------------------------------------
    | CSV Plugin     | ElasticSearch    |
    -------------------------------------
    | master         | 1.5.x -> master  |
    -------------------------------------
    | 2.2.0          | 1.5.x -> master  |
    -------------------------------------
    | 2.1.2          | 1.4.x -> master  |
    -------------------------------------
    | 2.0.2          | 1.0.x -> 1.2.x   |
    -------------------------------------
    | 2.0.1          | 1.0.x -> 1.2.x   |
    -------------------------------------
    | 2.0.0          | 1.0.0            |
    -------------------------------------
    | 1.0.1          | 0.19.x           |
    -------------------------------------
    | 1.0.0          | 0.19.x           |
    -------------------------------------

The CSV river import data from CSV files and index it.

##Changelog

###2.2.1-SNAPSHOT

* no changes yet

###2.2.0

* updated dependencies to latest versions

###2.1.2

* fixed - After import, the file remained open, preventing a rename to .imported.
* added the ability to run bash, powershell and "Windows script host"

###2.1.1

* added support for custom charset
* added support for generating an import timestamp in all imported documents
* made list of files to be imported / which have been imported visible to the before/after listener scripts

###2.1.0

* Works with ElasticSearch 1.2.x - 1.3.x

###2.0.2

* Minor enhancements

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
            "field_id_include" : "false",
            "field_timestamp" : "imported_at",
            "concurrent_requests" : "1",
            "charset" : "UTF-8",
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

* takes list of all file paths as arguments

	    "script_before_all": "/path/to/before_all.sh"
    	"script_after_all": "/path/to/after_all.sh"

* file path is argument

        "script_before_file": "/path/to/before_file.sh",
        "script_after_file": "/path/to/after_file.sh"

####Examples how the files look like

        #!/bin/sh
        echo "greetings from shell before all, will process $*"


        #!/bin/bash
        echo "greetings from shell before file $1"


        #!/bin/bash
        echo "greetings from shell after file $1"


        #!/bin/bash
        echo "greetings from shell after all, processed $*"


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
    | field_separator   | ,   (for tab separator use ```\t```     |
    ----------------------------------------
    | charset           | UTF-8            |
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
    
####Charset
 
Default charset is "UTF-8". If you need different, consider to use one of:
 
* US-ASCII	Seven-bit ASCII, a.k.a. ISO646-US, a.k.a. the Basic Latin block of the Unicode character set
* ISO-8859-1  	ISO Latin Alphabet No. 1, a.k.a. ISO-LATIN-1
* UTF-8	    Eight-bit UCS Transformation Format
* UTF-16BE	Sixteen-bit UCS Transformation Format, big-endian byte order
* UTF-16LE	Sixteen-bit UCS Transformation Format, little-endian byte order
* UTF-16 	Sixteen-bit UCS Transformation Format, byte order identified by an optional byte-order mark
 
 More details about charsets are available at http://docs.oracle.com/javase/6/docs/api/java/nio/charset/Charset.html


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
