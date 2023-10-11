package com.twitter.stream.twitter.to.kafka.server.runner.impl;

import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import com.twitter.stream.config.TwitterToKafkaServiceConfigData;
import com.twitter.stream.twitter.to.kafka.server.listener.TwitterKafkaStatusListener;
import com.twitter.stream.twitter.to.kafka.server.runner.StreamRunner;

import jakarta.annotation.PreDestroy;
import twitter4j.FilterQuery;
import twitter4j.TwitterException;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;

@Component
@ConditionalOnProperty(name = "twitter-to-kafka-service.enable-mock-tweets", havingValue = "false", matchIfMissing = true)
public class TwitterKafkaStreamRunner implements StreamRunner {
	
	private static final Logger LOG = LoggerFactory.getLogger(TwitterKafkaStreamRunner.class);
	
	private final TwitterToKafkaServiceConfigData twitterToKafkaServiceConfigData;
	
	private final TwitterKafkaStatusListener twitterKafkaStatusListener;
	
	private TwitterStream twitterStream;
	
	public TwitterKafkaStreamRunner (
			TwitterToKafkaServiceConfigData twitterToKafkaServiceConfigData, 
			TwitterKafkaStatusListener twitterKafkaStatusListener) {
		
		this.twitterToKafkaServiceConfigData = twitterToKafkaServiceConfigData;
		this.twitterKafkaStatusListener = twitterKafkaStatusListener;
	}

	@Override
	public void start() throws TwitterException {
		
		LOG.info("start()");
		
		var keywordsListAsArray = twitterToKafkaServiceConfigData.getTwitterKeywordsAsArray();
		
		twitterStream = TwitterStreamFactory.getSingleton();
		twitterStream.addListener(twitterKafkaStatusListener);
		FilterQuery filterQuery = new FilterQuery(keywordsListAsArray);
        twitterStream.filter(filterQuery);
        LOG.info("Started filtering twitter stream for keywords {}", Arrays.toString(keywordsListAsArray));
	}
	
	@PreDestroy
	public void shutdown() {
		
		if (twitterStream != null) {
			LOG.info("closing twitter stream");
		}
		
	}

}
