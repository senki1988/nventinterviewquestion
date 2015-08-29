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

/**
 * The purpose of this filter is to allow only the messages with random field = {@link #VALUE}
 * @author senki
 *
 */
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
	
	/**
	 * Constructor
	 * @param value value to allow
	 * @param randomFieldName name of the random field on the avro schema
	 */
	public FilterByValue(String value, String randomFieldName) {
		RANDOM_FIELD_NAME = randomFieldName;
		VALUE=value;
	}
	
	/**
	 * Filter function: deserializes message by using avro schema and allow only those
	 * that has {@link #VALUE} value in the random field
	 */
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
