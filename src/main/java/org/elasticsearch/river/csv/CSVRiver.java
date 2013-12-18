/*
 *   This software is licensed under the Apache 2 license, quoted below.
 *
 *   Copyright 2012-2013 Martin Bednar
 *
 *   Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *   use this file except in compliance with the License. You may obtain a copy of
 *   the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *   WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *   License for the specific language governing permissions and limitations under
 *   the License.
 */

package org.elasticsearch.river.csv;

import static au.com.bytecode.opencsv.CSVParser.DEFAULT_ESCAPE_CHARACTER;
import static au.com.bytecode.opencsv.CSVParser.DEFAULT_QUOTE_CHARACTER;
import static au.com.bytecode.opencsv.CSVParser.DEFAULT_SEPARATOR;

import java.io.File;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.river.AbstractRiverComponent;
import org.elasticsearch.river.River;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;

import au.com.bytecode.opencsv.CSVReader;

/**
 *
 */
public class CSVRiver extends AbstractRiverComponent implements River {

	private final Client client;
	private final String indexName;
	private final String typeName;
	private final int bulkSize;
	private String folderName;
	private String filenamePattern;
	private volatile boolean closed = false;
	private List<Object> csvFields;
	private TimeValue poll;
	private Thread thread;
	private char escapeCharacter;
	private char quoteCharacter;
	private char separator;
	private int bulkThreshold;

	@SuppressWarnings({ "unchecked" })
	@Inject
	public CSVRiver(RiverName riverName, RiverSettings settings, Client client) {
		super(riverName, settings);
		this.client = client;

		if (settings.settings().containsKey("csv_file")) {
			Map<String, Object> csvSettings = (Map<String, Object>) settings
					.settings().get("csv_file");
			folderName = XContentMapValues.nodeStringValue(
					csvSettings.get("folder"), null);
			filenamePattern = XContentMapValues.nodeStringValue(
					csvSettings.get("filename_pattern"), ".*\\.csv$");
			csvFields = XContentMapValues.extractRawValues("fields",
					csvSettings);
			poll = XContentMapValues.nodeTimeValue(csvSettings.get("poll"),
					TimeValue.timeValueMinutes(60));
			escapeCharacter = XContentMapValues.nodeStringValue(
					csvSettings.get("escape_character"),
					String.valueOf(DEFAULT_ESCAPE_CHARACTER)).charAt(0);
			separator = XContentMapValues.nodeStringValue(
					csvSettings.get("field_separator"),
					String.valueOf(DEFAULT_SEPARATOR)).charAt(0);
			quoteCharacter = XContentMapValues.nodeStringValue(
					csvSettings.get("quote_character"),
					String.valueOf(DEFAULT_QUOTE_CHARACTER)).charAt(0);
		}

		logger.info("creating csv stream river for [{}] with pattern [{}]",
				folderName, filenamePattern);

		if (settings.settings().containsKey("index")) {
			Map<String, Object> indexSettings = (Map<String, Object>) settings
					.settings().get("index");
			indexName = XContentMapValues.nodeStringValue(
					indexSettings.get("index"), riverName.name());
			typeName = XContentMapValues.nodeStringValue(
					indexSettings.get("type"), "csv_type");
			bulkSize = XContentMapValues.nodeIntegerValue(
					indexSettings.get("bulk_size"), 100);
			bulkThreshold = XContentMapValues.nodeIntegerValue(
					indexSettings.get("bulk_threshold"), 10);
		} else {
			indexName = riverName.name();
			typeName = "csv_type";
			bulkSize = 100;
			bulkThreshold = 10;
		}
	}

	@Override
	public void start() {
		logger.info("starting csv stream");

		thread = EsExecutors.daemonThreadFactory(settings.globalSettings(),
				"CSV processor").newThread(new CSVConnector());
		thread.start();
	}

	@Override
	public void close() {
		logger.info("closing csv stream river");
		this.closed = true;
		thread.interrupt();
	}

	private void delay() {
		if (poll.millis() > 0L) {
			logger.info("next run waiting for {}", poll);
			try {
				Thread.sleep(poll.millis());
			} catch (InterruptedException e) {
				logger.error("Error during waiting.", e, (Object) null);
			}
		}
	}

	private class CSVConnector implements Runnable {

		@Override
		public void run() {
			while (!closed) {
				File lastProcessedFile = null;
				try {
					File files[] = getFiles();
					for (File file : files) {
						logger.info("Processing file {}", file.getName());
						file = renameFile(file, ".processing");
						lastProcessedFile = file;

						processFile(file);

						file = renameFile(file, ".imported");
						lastProcessedFile = file;
					}
					delay();
				} catch (Exception e) {
					if (lastProcessedFile != null) {
						renameFile(lastProcessedFile, ".error");
					}
					logger.error(e.getMessage(), e, (Object) null);
					closed = true;
				}
				if (closed) {
					return;
				}
			}
		}

		private File renameFile(File file, String suffix) {
			File newFile = new File(file.getAbsolutePath() + suffix);
			if (!file.renameTo(newFile)) {
				logger.error("can't rename file {} to {}", file.getName(),
						newFile.getName());
			}
			return newFile;
		}

		private File[] getFiles() {
			File folder = new File(folderName);
			return folder.listFiles(new FilenameFilter() {
				@Override
				public boolean accept(File file, String s) {
					return s.matches(filenamePattern);
				}
			});
		}

		private void processFile(File file) throws IOException {
			CSVReader reader = new CSVReader(new FileReader(file), separator,
					quoteCharacter, escapeCharacter);
			BulkProcessor bulkProcessor = getBulkProcessor();
			String[] nextLine;
			while ((nextLine = reader.readNext()) != null) {
				if (nextLine.length > 0
						&& !(nextLine.length == 1 && nextLine[0].trim().equals(
								""))) {
					XContentBuilder builder = XContentFactory.jsonBuilder();
					builder.startObject();

					int position = 0;
					for (Object fieldName : csvFields) {
						builder.field((String) fieldName, nextLine[position++]);
					}
					builder.endObject();
					bulkProcessor.add(Requests.indexRequest(indexName)
							.type(typeName).id(UUID.randomUUID().toString())
							.create(true).source(builder));
				}
			}
			reader.close();
			bulkProcessor.close();
		}
	}

	private BulkProcessor getBulkProcessor() {
		return BulkProcessor
				.builder(client, new BulkProcessor.Listener() {

					@Override
					public void beforeBulk(long executionId, BulkRequest request) {
						logger.info(
								"Going to execute new bulk composed of {} actions",
								request.numberOfActions());

					}

					@Override
					public void afterBulk(long executionId,
							BulkRequest request, Throwable failure) {
						logger.warn("Error executing bulk", failure);
					}

					@Override
					public void afterBulk(long executionId,
							BulkRequest request, BulkResponse response) {
						logger.info("Executed bulk composed of {} actions",
								request.numberOfActions());
					}
				})
				.setBulkActions(bulkSize)
				.setBulkSize(
						ByteSizeValue.parseBytesSizeValue(
								String.valueOf(bulkThreshold),
								new ByteSizeValue(5000)))
				.setConcurrentRequests(5).build();
	}
}
