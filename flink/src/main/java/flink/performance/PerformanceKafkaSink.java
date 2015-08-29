package flink.performance;

import java.util.Properties;

import org.apache.avro.generic.GenericRecord;
import org.apache.flink.streaming.connectors.kafka.api.KafkaSink;
import org.apache.flink.streaming.util.serialization.SerializationSchema;
/**
 * The purpose of this class is to wrap {@link KafkaSink} 
 * and counts written message for performance measurement after sending it to kafka
 * @author senki
 *
 * @param <IN> sink input datatype
 */
public class PerformanceKafkaSink<IN> extends KafkaSink<GenericRecord> {
	
	/**
	 * Serialization ID
	 */
	private static final long serialVersionUID = 1L;

	public PerformanceKafkaSink(String brokerList, String topicId, Properties producerConfig,
			SerializationSchema<GenericRecord, byte[]> serializationSchema) {
		super(brokerList, topicId, producerConfig, serializationSchema);
	}
	
	/**
	 * After adding message to the target topic, the message is marked written
	 */
	@Override
	public void invoke(GenericRecord next) {
		super.invoke(next);
		PerformanceMeter.messageWritten(23);
	}

}
