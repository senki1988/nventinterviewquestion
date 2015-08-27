package flink.function;

import java.util.Arrays;

import org.apache.flink.streaming.api.collector.selector.OutputSelector;

public class ClonerSelector implements OutputSelector<byte[]> {

	/**
	 * Serialization ID
	 */
	private static final long serialVersionUID = 1L;
	
	/**
	 * Splits stream to first and second
	 */
	@Override
	public Iterable<String> select(byte[] value) {
		return Arrays.asList("first", "second");
	}

}
