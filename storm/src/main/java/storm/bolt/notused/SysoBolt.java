package storm.bolt.notused;

import java.nio.ByteBuffer;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.TupleImpl;
import kafka.message.Message;

public class SysoBolt extends BaseRichBolt {
	
	private static final long serialVersionUID = 1L;

	private OutputCollector collector;
	
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	protected String getMessage(Tuple input) {
		// Kafka message
		Message message = new Message((byte[])((TupleImpl) input).get("bytes"));
	
		// get message payload
	    ByteBuffer bb = message.payload();
	    
	    // create byte[] from payload
	    byte[] b = new byte[bb.remaining()];
	    bb.get(b, 0, b.length);
	    
	    StringBuilder sb = new StringBuilder();
	    
	    for(byte singleByte: b) {
	    	sb.append(String.format("%02x", singleByte));
	    }
	    
	    return sb.toString();
	}

	public void execute(Tuple input) {
        System.out.println("Result: " + getMessage((Tuple)(input.getValue(1))));
        System.out.println("Goes to: " + input.getString(0));

        collector.ack(input);
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

}
