package com.twitter.stream.twitter.to.kafka.server.runner;

import twitter4j.TwitterException;

public interface StreamRunner {
	
	void start() throws TwitterException;

}
