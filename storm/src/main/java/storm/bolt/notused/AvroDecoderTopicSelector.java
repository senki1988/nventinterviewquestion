package storm.bolt.notused;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;

import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.TupleImpl;
import kafka.message.Message;
import storm.kafka.bolt.selector.KafkaTopicSelector;

public class AvroDecoderTopicSelector implements KafkaTopicSelector {

	private static final long serialVersionUID = 1L;

	private Schema _schema;
	
	private static final String RANDOM_FIELD_NAME = "random";
	private final String TOPIC_PREFIX;
	
	public AvroDecoderTopicSelector(String topicPrefix) {
		this.TOPIC_PREFIX = topicPrefix;
		Schema.Parser parser = new Schema.Parser();
		try {
			_schema = parser.parse(getClass().getResourceAsStream("/kafkatest.avsc"));
			
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	
	protected byte[] getMessage(Tuple input) {
		// Kafka message
		Message message = new Message((byte[])((TupleImpl) input).get("bytes"));
	
		// get message payload
	    ByteBuffer bb = message.payload();
	    
	    // create byte[] from payload
	    byte[] b = new byte[bb.remaining()];
	    bb.get(b, 0, b.length);
	    return b;
	}

	@Override
	public String getTopic(Tuple tuple) {
		String random = null;
		byte[] kafkaMessage = getMessage(tuple);
		
        try {
            DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(_schema);
			Decoder decoder = DecoderFactory.get().binaryDecoder(kafkaMessage, null);
            GenericRecord result = reader.read(null, decoder);
            
            // TODO: check format
            random = result.get(RANDOM_FIELD_NAME).toString();
            
            return TOPIC_PREFIX + random;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
	}

}
