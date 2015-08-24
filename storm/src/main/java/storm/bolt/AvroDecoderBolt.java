package storm.bolt;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.TupleImpl;
import kafka.message.Message;

/**
 * The purpose of this class is to decode a message from kafka by using an avro scheme
 * @author senki
 *
 */
public class AvroDecoderBolt extends BaseRichBolt {
	/**
	 * Serialization id
	 */
	private static final long serialVersionUID = 1L;
	
	/**
	 * Spout collector that is used to emit and ack storm messages
	 */
	private OutputCollector collector;
	
	/**
	 * Avro scheme
	 */
	private Schema schema;
	
	/**
	 * The name of the field in the avro scheme that contains the value on which the separation is done. 
	 */
	private final String RANDOM_FIELD_NAME;
	
	/**
	 * Flag for indicating performance test mode
	 */
	private final boolean PERFORMANCE_TEST;
	
	/**
	 * Constructor
	 * @param perftest flag for performance test mode
	 * @param randomFieldName name of the field in the avro scheme that contains the value on which the separation is done. 
	 */
	public AvroDecoderBolt(String randomFieldName, boolean perftest) {
		PERFORMANCE_TEST = perftest;
		RANDOM_FIELD_NAME = randomFieldName;
	}
	
	/**
	 * Constructor without performance test mode
	 * @param randomFieldName name of the field in the avro scheme that contains the value on which the separation is done. 
	 */
	public AvroDecoderBolt(String randomFieldName) {
		this(randomFieldName, false);
	}
	
	/**
	 * Bolt prepare. Collector and avro schema is set.
	 */
	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
        Schema.Parser parser = new Schema.Parser();
        try {
            schema = parser.parse(getClass().getResourceAsStream("/kafkatest.avsc"));

        } catch (IOException e) {
            throw new RuntimeException(e);
        }

	}
	
	/**
	 * Helper function to obtain the byte array from kafka message
	 * @param input tuple that contains kafka message
	 * @return kafka message as byte array
	 */
	private byte[] getMessage(Tuple input) {
		// Kafka message
		Message message = new Message((byte[])((TupleImpl) input).get("bytes"));
	
		// get message payload
	    ByteBuffer bb = message.payload();
	    
	    // create byte[] from payload
	    byte[] b = new byte[bb.remaining()];
	    bb.get(b, 0, b.length);
	    return b;
	}

	/**
	 * Bolt execution. Kafka message is transformed into avro schema to obtain the random field.
	 * This random field is extracted and emitted together with the original kafka message.
	 * It contains timestamp if {@link #PERFORMANCE_TEST} is set
	 */
	@Override
	public void execute(Tuple input) {
		// value of random field
		String random = null;
		// binary kafka message
		byte[] kafkaMessage = getMessage(input);
		
        try {
        	// parsing kafka message based on the avro schema
            DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema);
			Decoder decoder = DecoderFactory.get().binaryDecoder(kafkaMessage, null);
            GenericRecord result = reader.read(null, decoder);
            
            random = result.get(RANDOM_FIELD_NAME).toString();
            // format check
            if(!random.matches("\\d+")) {
            	throw new NumberFormatException("Value of " + RANDOM_FIELD_NAME + " is not a number");
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        
        // composing tuple to emit
        List<Object> delegatedTuple = new ArrayList<Object>();
        delegatedTuple.add(random);
        // TODO: check if kafka message can be sent
        //delegatedTuple.add(new String(kafkaMessage));
        delegatedTuple.add(kafkaMessage);
        if(PERFORMANCE_TEST) {
        	delegatedTuple.add(input.getLongByField("timestamp"));
        }
		collector.emit(delegatedTuple);
        collector.ack(input);
	}

	/**
	 * Output fields of the tuple. It contains timestamp if {@link #PERFORMANCE_TEST} is set
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		List<String> fields = new ArrayList<String>();
		fields.add("random");
		fields.add("message");
		if(PERFORMANCE_TEST) {
			fields.add("timestamp");
		}
		declarer.declare(new Fields(fields));
	}

}
