package com.twitter.stream.kafka.admin.config.client;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicListing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.ClientResponse;
import org.springframework.web.reactive.function.client.WebClient;

import com.twitter.stream.config.KafkaConfigData;
import com.twitter.stream.kafka.admin.exception.KafkaClientException;

@Component
public class KafkaAdminClient {

	private static final Logger LOG = LoggerFactory.getLogger(KafkaAdminClient.class);

	private final KafkaConfigData kafkaConfigData;


	private final AdminClient adminClient;

	private final WebClient webClient;

	public KafkaAdminClient(KafkaConfigData config, 
			AdminClient client,
			WebClient webClient) {
		this.kafkaConfigData = config;
		this.adminClient = client;
		this.webClient = webClient;
	}
	
	public void createTopics() {
		CreateTopicsResult createTopicsResult;
		try {
			createTopicsResult = doCreateTopics();
			LOG.info("Create topic result {}", createTopicsResult.values().values());
		} catch (Throwable t) {
			throw new KafkaClientException("Reached max number of retry for creating kafka topic(s)!", t);
		}
		checkTopicsCreated();
	}

	public void checkTopicsCreated() {
		Collection<TopicListing> topics = getTopics();
		int retryCount = 1;
		
		for (String topic : kafkaConfigData.getTopicNamesToCreate()) {
			
		}
	}

	public void checkSchemaRegistry() {
		int retryCount = 1;
		
	}

	private HttpStatusCode getSchemaRegistryStatus() {
		try {
			return webClient
                    .method(HttpMethod.GET)
                    .uri(kafkaConfigData.getSchemaRegistryUrl())
                    .exchange()
                    .map(ClientResponse::statusCode)
                    .block();
		} catch (Exception e) {
			return HttpStatus.SERVICE_UNAVAILABLE;
		}
	}

	private void sleep(Long sleepTimeMs) {
		try {
			Thread.sleep(sleepTimeMs);
		} catch (InterruptedException e) {
			throw new KafkaClientException("Error while sleeping for waiting new created topics!!");
		}
	}

	private void checkMaxRetry(int retry, Integer maxRetry) {
		if (retry > maxRetry) {
			throw new KafkaClientException("Reached max number of retry for reading kafka topic(s)!");
		}
	}

	private boolean isTopicCreated(Collection<TopicListing> topics, String topicName) {
		if (topics == null) {
			return false;
		}
		return topics.stream().anyMatch(topic -> topic.name().equals(topicName));
	}

	private CreateTopicsResult doCreateTopics() {
		List<String> topicNames = kafkaConfigData.getTopicNamesToCreate();
		
		
		List<NewTopic> kafkaTopics = topicNames.stream().map(topic -> new NewTopic(
				topic.trim(), // topic name
				kafkaConfigData.getNumOfPartitions(), // number of partitions 
				kafkaConfigData.getReplicationFactor())) // replication factor
				.collect(Collectors.toList());
		
		return adminClient.createTopics(kafkaTopics);
	}

	private Collection<TopicListing> getTopics() {
		Collection<TopicListing> topics;
		try {
			topics = doGetTopics();			
		} catch (Throwable t) {
			throw new KafkaClientException("Reached max number of retry for reading kafka topic(s)!", t);
		}
		return topics;
	}

	private Collection<TopicListing> doGetTopics()
			throws ExecutionException, InterruptedException {
		Collection<TopicListing> topics = adminClient.listTopics().listings().get();
		if (topics != null) {
			topics.forEach(topic -> LOG.debug("Topic with name {}", topic.name()));
		}
		return topics;
	}
}
