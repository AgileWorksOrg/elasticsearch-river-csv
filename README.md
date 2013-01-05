Twitter River Plugin for ElasticSearch
==================================

The CSV River plugin allows index CSV files in folder.

In order to install the plugin, simply run: `bin/plugin -install elasticsearch/elasticsearch-river-csv/1.0.0`.

    -------------------------------------
    | CSV Plugin | ElasticSearch        |
    -------------------------------------
    | master         | 0.19.x -> master |
    -------------------------------------
    | 1.0.0          | 0.19.x           |
    -------------------------------------

The CSV river import data from CSV files and index it.

Creating the CSV river can be done using:

	curl -XPUT localhost:9200/_river/my_csv_river/_meta -d '
	{
	    "type" : "csv",
	    "csv_file" : {
	        "folder" : "/tmp",
	        "filename_mask" : ".*\\.csv$",
	        "poll":"5m",
	        "fields" : [
	            "column1",
	            "column2",
	            "column3",
	            "column4"
	        ],
   	        "field_separator" : ",",
   	        "escape_character" : ";",
   	        "quote_character" : "'"
	    },
	    "index" : {
	        "index" : "my_csv_data",
	        "type" : "csv_row",
	        "bulk_size" : 10000,
	    }
	}'


License
-------

    This software is licensed under the Apache 2 license, quoted below.

    Copyright 2012-2013 Martin Bednar

    Licensed under the Apache License, Version 2.0 (the "License"); you may not
    use this file except in compliance with the License. You may obtain a copy of
    the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
    License for the specific language governing permissions and limitations under
    the License.
