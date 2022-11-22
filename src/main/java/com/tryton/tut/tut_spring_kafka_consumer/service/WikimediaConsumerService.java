package com.tryton.tut.tut_spring_kafka_consumer.service;

import com.google.gson.JsonParser;
import lombok.RequiredArgsConstructor;
import lombok.extern.apachecommons.CommonsLog;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;

@Service
@CommonsLog
@RequiredArgsConstructor
public class WikimediaConsumerService {

	private final RestHighLevelClient openSearchClient;
	private final Consumer<String, String> consumer;

	public void start() throws IOException {
		// we need to create the index on OpenSearch if it doesn't exist already
		try (openSearchClient; consumer) {

			boolean indexExists = openSearchClient.indices().exists(new GetIndexRequest("wikimedia"), RequestOptions.DEFAULT);

			if (!indexExists) {
				CreateIndexRequest createIndexRequest = new CreateIndexRequest("wikimedia");
				openSearchClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
				log.info("The Wikimedia Index has been created!");
			} else {
				log.info("The Wikimedia Index already exits");
			}

			// we subscribe the consumer
			consumer.subscribe(Collections.singleton("wikimedia.recentchange"));


			while (true) {
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(3000));

				int recordCount = records.count();
				log.info("Received " + recordCount + " record(s)");

				BulkRequest bulkRequest = new BulkRequest();

				for (ConsumerRecord<String, String> record : records) {
					// send the record into OpenSearch
					// strategy 1
					// define an ID using Kafka Record coordinates
//                    String id = record.topic() + "_" + record.partition() + "_" + record.offset();

					try {
						// strategy 2
						// we extract the ID from the JSON value
						String id = extractId(record.value());

						IndexRequest indexRequest = new IndexRequest("wikimedia")
								.source(record.value(), XContentType.JSON)
								.id(id);

//                        IndexResponse response = openSearchClient.index(indexRequest, RequestOptions.DEFAULT);
						bulkRequest.add(indexRequest);
//                        log.info(response.getId());
					} catch (Exception e) {
						log.error("Exception occured: " + e.getMessage(), e);
					}

				}

				if (bulkRequest.numberOfActions() > 0) {
					BulkResponse bulkResponse = openSearchClient.bulk(bulkRequest, RequestOptions.DEFAULT);
					log.info("Inserted " + bulkResponse.getItems().length + " record(s).");

					// commit offsets after the batch is consumed
					consumer.commitSync();
					log.info("Offsets have been committed!");
				}
			}
		}
	}

	private static String extractId(String json) {
		// gson library
		return JsonParser.parseString(json)
				.getAsJsonObject()
				.get("meta")
				.getAsJsonObject()
				.get("id")
				.getAsString();
	}
}
