package com.twitter.stream.twitter.to.kafka.server.config;

import java.util.List;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import lombok.Data;

@Data
@Configuration
@ConfigurationProperties(prefix = "twitter-to-kafka-service")
public class TwitterToKafkaServiceConfigData {
	private List<String> twitterKeywords;
	private String welcomeMessage;
    private Long mockSleepMs;
    private Integer mockMinTweetLength;
    private Integer mockMaxTweetLength;
	
	public String[] getTwitterKeywordsAsArray() {
		
		return twitterKeywords.toArray(new String[0]);		
	}

}
