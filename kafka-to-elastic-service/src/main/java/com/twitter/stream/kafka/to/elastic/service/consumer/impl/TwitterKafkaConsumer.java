package com.twitter.stream.kafka.to.elastic.service.consumer.impl;

import java.util.List;
import java.util.Objects;

import org.bouncycastle.util.Integers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import com.twitter.stream.config.KafkaConfigData;
import com.twitter.stream.kafka.admin.config.client.KafkaAdminClient;
import com.twitter.stream.kafka.avro.model.TwitterAvroModel;
import com.twitter.stream.kafka.to.elastic.service.consumer.KafkaConsumer;

@Service
public class TwitterKafkaConsumer implements KafkaConsumer<Long, TwitterAvroModel> {
	
	private static final Logger LOG = LoggerFactory.getLogger(TwitterKafkaConsumer.class);
	
	private final KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;
	
	private final KafkaAdminClient kafkaAdminClient;
	
	private final KafkaConfigData kafkaConfigData;

	public TwitterKafkaConsumer(KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry,
			KafkaAdminClient kafkaAdminClient, KafkaConfigData kafkaConfigData) {
		super();
		this.kafkaListenerEndpointRegistry = kafkaListenerEndpointRegistry;
		this.kafkaAdminClient = kafkaAdminClient;
		this.kafkaConfigData = kafkaConfigData;
	}
	
	@EventListener
	public void OnAppStarted(ApplicationStartedEvent event) {
		kafkaAdminClient.checkTopicsCreated();
		LOG.info("Topics with name {} is ready for operations!", kafkaConfigData.getTopicNamesToCreate().toArray());
        //kafkaListenerEndpointRegistry.getListenerContainer(kafkaConsumerConfigData.getConsumerGroupId())).start();
		kafkaListenerEndpointRegistry.getListenerContainer("twitterTopicListener").start();
	}



	@Override
	@KafkaListener(id = "twitterTopicListener", topics = "${kafka-config.topic-name}")
	public void receive(
			@Payload List<TwitterAvroModel> messages, 
			@Header(KafkaHeaders.RECEIVED_KEY) List<Integers> keys, 
			@Header(KafkaHeaders.RECEIVED_PARTITION) List<Integers> partitions,
			@Header(KafkaHeaders.OFFSET) List<Long> offsets) {
		
		LOG.info("{} number of message received with keys {}, partitions {} and offsets {}, " +
                "sending it to elastic: Thread id {}",
        messages.size(),
        keys.toString(),
        partitions.toString(),
        offsets.toString(),
        Thread.currentThread().getId());
		//List<TwitterIndexModel> twitterIndexModels = avroToElasticModelTransformer.getElasticModels(messages);
		//List<String> documentIds = elasticIndexClient.save(twitterIndexModels);
		//LOG.info("Documents saved to elasticsearch with ids {}", documentIds.toArray());		
	}

}
