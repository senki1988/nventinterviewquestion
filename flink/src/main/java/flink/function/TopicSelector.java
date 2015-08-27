package flink.function;

import java.util.Arrays;

import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;

/**
 * The purpose of this class is to create tuple from the random field value and the GenericRecord itself
 * @author senki
 *
 */
public class TopicSelector implements OutputSelector<Tuple2<String, byte[]>> {

	/**
	 * Serialization ID
	 */
	private static final long serialVersionUID = 1L;

	/**
	 * Returns the first value of the tuple, that contains the value for the random field
	 */
	@Override
	public Iterable<String> select(Tuple2<String, byte[]> value) {
		return Arrays.asList(value.f0);
	}

}
