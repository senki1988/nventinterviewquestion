package flink.function;

import java.util.Arrays;

import org.apache.avro.generic.GenericRecord;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;

public class ClonerSelector implements OutputSelector<GenericRecord> {

	/**
	 * Serialization ID
	 */
	private static final long serialVersionUID = 1L;
	
	/**
	 * Select topic based on the value of the field {@link #RANDOM_FIELD_NAME}
	 */
	@Override
	public Iterable<String> select(GenericRecord value) {
		return Arrays.asList("first", "second");
	}

}
