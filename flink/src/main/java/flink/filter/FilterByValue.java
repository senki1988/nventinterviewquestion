package flink.filter;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.flink.api.common.functions.FilterFunction;

import flink.mapper.ExtractFieldFlatMapper;

public class FilterByValue implements FilterFunction<byte[]> {

	/**
	 * Serialization ID
	 */
	private static final long serialVersionUID = 1L;
	
	/**
	 * Specific value for the random field
	 */
	private final String VALUE;
	
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
			schema = parser.parse(ExtractFieldFlatMapper.class.getResourceAsStream("/kafkatest.avsc"));
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	
	public FilterByValue(String value, String randomFieldName) {
		RANDOM_FIELD_NAME = randomFieldName;
		VALUE=value;
	}
	

	@Override
	public boolean filter(byte[] value) throws Exception {
		try {
			DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema);
			Decoder decoder = DecoderFactory.get().binaryDecoder(value, null);
			GenericRecord record = reader.read(null, decoder);
			return VALUE.equals(record.get(RANDOM_FIELD_NAME).toString());
		} catch (IOException e) {
			throw new RuntimeException(e); 
		}
	}

}
