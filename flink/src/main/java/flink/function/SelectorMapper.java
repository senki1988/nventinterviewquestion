package flink.function;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * The purpose of this class is to get kafka message part of the input tuple
 * @author senki
 *
 */
public class SelectorMapper implements FlatMapFunction<Tuple2<String, byte[]>, byte[]> {

	/**
	 * Serialization ID
	 */
	private static final long serialVersionUID = 1L;
	
	/**
	 * Selecting only the kafka message part of the tuple
	 */
	@Override
	public void flatMap(Tuple2<String, byte[]> value, Collector<byte[]> out) {
		out.collect(value.f1);
	}

}
