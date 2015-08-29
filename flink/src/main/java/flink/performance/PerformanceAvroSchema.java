package flink.performance;

import org.apache.avro.generic.GenericRecord;

import flink.schema.AvroSchema;

/**
 * The purpose of this class is to wrap {@link AvroSchema} and measure performance
 * @author senki
 *
 */
public class PerformanceAvroSchema extends AvroSchema {

	/**
	 * Serialization ID
	 */
	private static final long serialVersionUID = 1L;
	
	/**
	 * Stores that the message has been read and calls super implementation
	 */
	@Override
	public GenericRecord deserialize(byte[] message) {
		PerformanceMeter.messageRead();
		return super.deserialize(message);
	}

}
