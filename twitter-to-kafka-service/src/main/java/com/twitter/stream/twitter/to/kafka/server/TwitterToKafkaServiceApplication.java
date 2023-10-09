package com.twitter.stream.twitter.to.kafka.server;

import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

import com.twitter.stream.config.TwitterToKafkaServiceConfigData;
import com.twitter.stream.twitter.to.kafka.server.init.StreamInitializer;
import com.twitter.stream.twitter.to.kafka.server.runner.StreamRunner;

@SpringBootApplication
@ComponentScan(basePackages = "com.twitter.stream") // allow for scanning in all modules so that config can be a bean
public class TwitterToKafkaServiceApplication implements CommandLineRunner {
	
	private static final Logger LOG = LoggerFactory.getLogger(TwitterToKafkaServiceApplication.class);
	
	
	private final StreamRunner streamRunner;
	
	private final StreamInitializer streamInitializer;
	
	public TwitterToKafkaServiceApplication(StreamRunner streamRunner, StreamInitializer streamInitializer) {
		this.streamRunner = streamRunner;
		this.streamInitializer = streamInitializer;
	}


	public static void main(String[] args) {
		
		SpringApplication.run(TwitterToKafkaServiceApplication.class, args);		
	}

	@Override
	public void run(String... args) throws Exception {
		LOG.info("Test");
		streamInitializer.init();
		streamRunner.start();
	}
	
}
