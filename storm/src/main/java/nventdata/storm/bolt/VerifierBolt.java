package nventdata.storm.bolt;

import java.util.ArrayList;
import java.util.GregorianCalendar;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

/**
 * The purpose of this class is to verify if all the messages are arrived to the separated topics.
 * One VerifierBolt deals with a single value of random.
 * @author senki
 *
 */
public class VerifierBolt extends BaseRichBolt {
	/**
	 * Serialization id
	 */
	private static final long serialVersionUID = 1L;
	
	/**
	 * Spout collector that is used to emit and ack storm messages
	 */
	private OutputCollector collector;
	
	/**
	 * Stores the messages that are arrived from the source topic. This is a buffer.
	 */
	private List<String> fromSource = new LinkedList<String>();
	
	/**
	 * Stores the messages that are arrived from the target topic. This is a buffer.
	 */
	private List<String> fromTarget = new LinkedList<String>();
	
	/**
	 * The value that this VerifierBolt deals with
	 */
	private final String VALUE;
	
	/**
	 * Constructor
	 * @param value value of the random field
	 */
	public VerifierBolt(String value) {
		VALUE = value;
	}
	
	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		// source messages are received by all the VerifierBolts, 
		// but a filtering on the given random value is needed for the messages from the source topic
		if(!VALUE.equals(input.getStringByField("random")) && !input.getSourceComponent().startsWith("verifier")) {
			collector.ack(input);
			return;
		}
		
		// if the source is verifier, then the message was read from the target topic
		if(input.getSourceComponent().startsWith("verifier")) {
			handleLists(fromTarget, fromSource, new String(input.getBinaryByField("message")));
		} else { // otherwise it is read from the source topic
			handleLists(fromSource, fromTarget, new String(input.getBinaryByField("message")));
		}
		
		// tuple is created: which value, when was it modified, is it verified or not?
		List<Object> result = new ArrayList<Object>(3);
		result.add(VALUE);
		result.add((new GregorianCalendar()).getTimeInMillis()); // current timestamp
		result.add(fromSource.isEmpty() && fromTarget.isEmpty());
		collector.emit(result);

        collector.ack(input);
	}

	/**
	 * Handles the buffer. If a message is in the "from" buffer, it is removed, 
	 * otherwise it is added to the "to" buffer 
	 * @param addTo buffer for adding a message
	 * @param removeFrom buffer for removing a message
	 * @param message the message itself
	 */
	private void handleLists(List<String> addTo, List<String> removeFrom, String message) {
		if(removeFrom.contains(message)) {
			removeFrom.remove(message);
		} else {
			addTo.add(message);
		}
		
	}

	/**
	 * Three values are in a tuple: 
	 * <ul>
	 * <li>the value of the random field</li>
	 * <li>the timestamp of the current timestamp</li>
	 * <li>the flag if the verification passed or not</li>
	 * </ul> 
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("random","timestamp","verified"));
	}

}
