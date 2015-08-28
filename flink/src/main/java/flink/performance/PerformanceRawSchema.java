package flink.performance;

import org.apache.flink.streaming.util.serialization.RawSchema;

/**
 * The purpose of this class is to wrap {@link RawSchema} and measure performance
 * @author senki
 *
 */
public class PerformanceRawSchema extends RawSchema {

	/**
	 * Serialization ID
	 */
	private static final long serialVersionUID = 1L;
	
	/**
	 * Stores that the message has been read and calls super implementation
	 */
	@Override
	public byte[] deserialize(byte[] message) {
		PerformanceMeter.messageRead();
		return super.deserialize(message);
	}

}
