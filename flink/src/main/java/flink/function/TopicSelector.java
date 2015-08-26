package flink.function;

import java.util.Arrays;

import org.apache.avro.generic.GenericRecord;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;

public class TopicSelector implements OutputSelector<GenericRecord> {

	/**
	 * Serialization ID
	 */
	private static final long serialVersionUID = 1L;
	/**
	 * The name of the field in the avro scheme that contains the value on which the separation is done. 
	 */
	private final String RANDOM_FIELD_NAME;

	/**
	 * Constructor
	 * @param randomFieldName the name of the field in the avro scheme that contains the value on which the separation is done
	 */
	public TopicSelector(String randomFieldName) {
		RANDOM_FIELD_NAME = randomFieldName;
	}

	/**
	 * Select topic based on the value of the field {@link #RANDOM_FIELD_NAME}
	 */
	@Override
	public Iterable<String> select(GenericRecord value) {
		return Arrays.asList(value.get(RANDOM_FIELD_NAME).toString());
	}

}
