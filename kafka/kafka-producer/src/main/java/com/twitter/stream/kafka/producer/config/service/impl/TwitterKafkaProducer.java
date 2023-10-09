package com.twitter.stream.kafka.producer.config.service.impl;

import java.util.concurrent.CompletableFuture;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import com.twitter.stream.kafka.avro.model.TwitterAvroModel;
import com.twitter.stream.kafka.producer.config.service.KafkaProducer;

import jakarta.annotation.PreDestroy;

@Service
public class TwitterKafkaProducer implements KafkaProducer<Long, TwitterAvroModel> {
	
	private static final Logger LOG = LoggerFactory.getLogger(TwitterKafkaProducer.class);

    private KafkaTemplate<Long, TwitterAvroModel> kafkaTemplate;

    public TwitterKafkaProducer(KafkaTemplate<Long, TwitterAvroModel> template) {
        this.kafkaTemplate = template;
    }

    @Override
    public void send(String topicName, Long key, TwitterAvroModel message) {
        LOG.info("Sending message='{}' to topic='{}'", message, topicName);
        CompletableFuture<SendResult<Long, TwitterAvroModel>> kafkaResultFuture = kafkaTemplate.send(topicName, key, message);      
        addCallback(topicName, message, kafkaResultFuture);
    }

    @PreDestroy
    public void close() {
        if (kafkaTemplate != null) {
            LOG.info("Closing kafka producer!");
            kafkaTemplate.destroy();
        }
    }

    private void addCallback(String topicName, TwitterAvroModel message,
    		CompletableFuture<SendResult<Long, TwitterAvroModel>> kafkaResultFuture) {
    	
        kafkaResultFuture.whenComplete((result, ex) -> {        	
        	if (ex == null) {
        		
        		RecordMetadata metadata = result.getRecordMetadata();
                LOG.debug("Received new metadata. Topic: {}; Partition {}; Offset {}; Timestamp {}, at time {}",
                        metadata.topic(),
                        metadata.partition(),
                        metadata.offset(),
                        metadata.timestamp(),
                        System.nanoTime());
                
                System.out.println("Sent message=[" + message + 
                    "] with offset=[" + result.getRecordMetadata().offset() + "]");
            } else {
            	LOG.error("Error while sending message {} to topic {}", message.toString(), topicName, ex.getMessage());
            }
        });
    }
}