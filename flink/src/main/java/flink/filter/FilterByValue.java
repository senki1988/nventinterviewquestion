package flink.filter;

import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.functions.FilterFunction;

/**
 * The purpose of this filter is to allow only the messages with random field = {@link #VALUE}
 * @author senki
 *
 */
public class FilterByValue implements FilterFunction<GenericRecord> {

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
	 * Constructor
	 * @param value value to allow
	 * @param randomFieldName name of the random field on the avro schema
	 */
	public FilterByValue(String value, String randomFieldName) {
		RANDOM_FIELD_NAME = randomFieldName;
		VALUE=value;
	}
	
	/**
	 * Filter function: allows only those messages that has {@link #VALUE} value in the random field
	 */
	@Override
	public boolean filter(GenericRecord value) throws Exception {
		return VALUE.equals(value.get(RANDOM_FIELD_NAME).toString());
	}

}
