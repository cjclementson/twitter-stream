package com.twitter.stream.kafka.to.elastic.service.consumer;

import java.io.Serializable;
import java.util.List;

import org.apache.avro.specific.SpecificRecordBase;
import org.bouncycastle.util.Integers;

public interface KafkaConsumer<K extends Serializable, V extends SpecificRecordBase> {
	
	void receive(List<V> messages, List<Integers> keys, List<Integers> partitions, List<Long> offsets);

}
