package storm.bolt;

import backtype.storm.tuple.Tuple;
import storm.kafka.bolt.selector.KafkaTopicSelector;

/**
 * Simple topic selector that returns the topic name based on the extracted value of random field
 * @author senki
 *
 */
public class SimpleTopicSelector implements KafkaTopicSelector {

	/**
	 * Serialization id
	 */
	private static final long serialVersionUID = 1L;

	/**
	 * Stores the topic prefix
	 */
	private final String TOPIC_PREFIX;
	
	/**
	 * Constructor
	 * @param topicPrefix topic prefix
	 */
	public SimpleTopicSelector(String topicPrefix) {
		TOPIC_PREFIX = topicPrefix;
	}
	
	/**
	 * Returns the topic name: &lt;topic prefix&gt;&lt;value of random field&gt;
	 */
	@Override
	public String getTopic(Tuple tuple) {
		return TOPIC_PREFIX + tuple.getStringByField("random");
	}

}
