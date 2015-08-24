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

public class AvroDecoderBolt extends BaseRichBolt {

	private static final long serialVersionUID = 1L;

	private OutputCollector _collector;
	private Schema _schema;
	
	private final String RANDOM_FIELD_NAME;
	
	private final boolean PERFORMANCE_TEST;
	
	public AvroDecoderBolt(boolean perftest, String randomFieldName) {
		PERFORMANCE_TEST = perftest;
		RANDOM_FIELD_NAME = randomFieldName;
	}
	
	public AvroDecoderBolt(String randomFieldName) {
		this(false, randomFieldName);
	}
	
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector) {
		_collector = collector;
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

	public void execute(Tuple input) {
		
		String random = null;
		byte[] kafkaMessage = getMessage(input);
		
        try {
            DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(_schema);
			Decoder decoder = DecoderFactory.get().binaryDecoder(kafkaMessage, null);
            GenericRecord result = reader.read(null, decoder);
            
            // TODO: check format
            random = result.get(RANDOM_FIELD_NAME).toString();
            
            //System.out.println("Result: " + result);
            //System.out.println("Goes to: " + random);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        
        
        List<Object> delegatedTuple = new ArrayList<Object>(2);
        delegatedTuple.add(random);
        // TODO: check if kafka message can be sent
        delegatedTuple.add(new String(kafkaMessage));
        if(PERFORMANCE_TEST) {
        	delegatedTuple.add(input.getLongByField("timestamp"));
        }
		_collector.emit(delegatedTuple);
        _collector.ack(input);
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO: performance flag
		List<String> fields = new ArrayList<String>();
		fields.add("random");
		fields.add("message");
		if(PERFORMANCE_TEST) {
			fields.add("timestamp");
		}
		declarer.declare(new Fields(fields));
	}

}
