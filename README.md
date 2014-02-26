CSV River Plugin for ElasticSearch
==================================

The CSV River plugin allows index CSV files in folder.

In order to install the plugin, simply run: `bin/plugin -install xxBedy/elasticsearch-river-csv/2.0.0`.
If it doesn't work, clone git repository and build plugin manually.

    -------------------------------------
    | CSV Plugin | ElasticSearch        |
    -------------------------------------
    | master         | 1.0.x -> master |
    -------------------------------------
    | 2.0.0          | 1.0.0            |
    -------------------------------------
    | 1.0.1          | 0.19.x           |
    -------------------------------------
    | 1.0.0          | 0.19.x           |
    -------------------------------------

The CSV river import data from CSV files and index it.

##Changelog

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
            "field_id" : "id"
	    },
	    "index" : {
	        "index" : "my_csv_data",
	        "type" : "csv_type",
	        "bulk_size" : 100,
	        "bulk_threshold" : 10
	    }
	}'


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
