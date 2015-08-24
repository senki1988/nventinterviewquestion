package storm.bolt;

import backtype.storm.tuple.Tuple;
import storm.kafka.bolt.selector.KafkaTopicSelector;

public class SimpleTopicSelector implements KafkaTopicSelector {

	private static final long serialVersionUID = 1L;

	private final String TOPIC_PREFIX;
	private static final String RANDOM_FIELD_NAME = "random";
	
	public SimpleTopicSelector(String topicPrefix) {
		this.TOPIC_PREFIX = topicPrefix;
	}
	
	@Override
	public String getTopic(Tuple tuple) {
		return TOPIC_PREFIX + tuple.getStringByField(RANDOM_FIELD_NAME);
	}

}
