package flink.function;

import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * The purpose of this class is to get GenericRecord part of the input tuple
 * @author senki
 *
 */
public class SelectorMapper implements FlatMapFunction<Tuple2<String, GenericRecord>, GenericRecord> {

	/**
	 * Serialization ID
	 */
	private static final long serialVersionUID = 1L;
	
	/**
	 * Selecting only the GenericRecord part of the tuple
	 */
	@Override
	public void flatMap(Tuple2<String, GenericRecord> value, Collector<GenericRecord> out) {
		out.collect(value.f1);
	}

}
