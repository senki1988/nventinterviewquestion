package flink.function;

import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * The purpose of this class is to provide functionality to extract a field value from the input avro scheme
 * and create a tuple of this value and the original record.
 * @author senki
 *
 */
public class ExtractFieldFlatMapper implements FlatMapFunction<GenericRecord, Tuple2<String, GenericRecord>> {

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
	public ExtractFieldFlatMapper(String randomFieldName) {
		RANDOM_FIELD_NAME = randomFieldName;
	}

	/**
	 * This function extract the {@link #RANDOM_FIELD_NAME} from the input and creates a tuple of that value and the whole record
	 */
	@Override
	public void flatMap(GenericRecord value, Collector<Tuple2<String, GenericRecord>> out) throws Exception {
		out.collect(new Tuple2<String, GenericRecord>(value.get(RANDOM_FIELD_NAME).toString(),value));
	}


}
