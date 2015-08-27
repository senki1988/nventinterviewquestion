package flink.function;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import flink.schema.AvroSchema;

/**
 * The purpose of this class is to provide functionality to extract a field value from the input avro scheme
 * and create a tuple of this value and the original record.
 * @author senki
 *
 */
public class ExtractFieldFlatMapper implements FlatMapFunction<byte[], Tuple2<String, byte[]>> {

	/**
	 * Serialization ID
	 */
	private static final long serialVersionUID = 1L;
	/**
	 * The name of the field in the avro scheme that contains the value on which the separation is done. 
	 */
	private final String RANDOM_FIELD_NAME;
	
	/**
	 * Avro schema
	 */
	private static Schema schema;
	static {
		Schema.Parser parser = new Schema.Parser();
		try {
			schema = parser.parse(AvroSchema.class.getResourceAsStream("/kafkatest.avsc"));
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	
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
	public void flatMap(byte[] value, Collector<Tuple2<String, byte[]>> out) throws Exception {
		try {
			DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema);
			Decoder decoder = DecoderFactory.get().binaryDecoder(value, null);
			GenericRecord record = reader.read(null, decoder);
			out.collect(new Tuple2<String, byte[]>(record.get(RANDOM_FIELD_NAME).toString(),value));
		} catch (IOException e) {
			throw new RuntimeException(e); 
		}
		
	}


}
