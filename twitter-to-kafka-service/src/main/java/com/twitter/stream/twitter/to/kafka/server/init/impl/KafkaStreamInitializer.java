package com.twitter.stream.twitter.to.kafka.server.init.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.twitter.stream.config.KafkaConfigData;
import com.twitter.stream.kafka.admin.config.client.KafkaAdminClient;
import com.twitter.stream.twitter.to.kafka.server.init.StreamInitializer;

@Component
public class KafkaStreamInitializer implements StreamInitializer {
	
	private static final Logger LOG = LoggerFactory.getLogger(KafkaStreamInitializer.class);
	private final KafkaConfigData kafkaConfigData;
	private final KafkaAdminClient kafkaAdminClient;
	
	public KafkaStreamInitializer(KafkaConfigData kafkaConfigData, KafkaAdminClient kafkaAdminClient) {
		super();
		this.kafkaConfigData = kafkaConfigData;
		this.kafkaAdminClient = kafkaAdminClient;
	}

	@Override
	public void init() {
		kafkaAdminClient.createTopics();
		kafkaAdminClient.checkSchemaRegistry();
		LOG.info("Topics with name {} is ready for operations!", kafkaConfigData.getTopicNamesToCreate().toArray());
		
	}
	

}
