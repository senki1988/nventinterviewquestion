package storm.bolt;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import backtype.storm.generated.Bolt;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

/**
 * The purpose of this class is to aggregate the results of the {@link VerifierBolt}s
 * @author senki
 *
 */
public class VerifierAggregatorBolt extends BaseRichBolt {
	
	/**
	 * Serialization id
	 */
	private static final long serialVersionUID = 1L;

	/**
	 * Spout collector that is used to emit and ack storm messages
	 */
	private OutputCollector collector;
	
	/**
	 * Stores the topic prefix
	 */
	private final String TOPIC_PREFIX;
	
	/**
	 * The purpose of this embedded class is to store the details of the results
	 * @author senki
	 *
	 */
	private class Result {
		/**
		 * Last modification time of the item
		 */
		private String time;
		
		/**
		 * Flag that indicates that the messages are verified at {@link #time}
		 */
		private boolean verified;
		
		/**
		 * Constructor
		 * @param time last modification time
		 * @param verified are the messages verified?
		 */
		public Result(String time, boolean verified) {
			this.time = time;
			this.verified = verified;
		}
		public String getTime() {
			return time;
		}
		public void setTime(String time) {
			this.time = time;
		}
		public boolean isVerified() {
			return verified;
		}
		public void setVerified(boolean verified) {
			this.verified = verified;
		}
		
	}
	
	/**
	 * Container for the results
	 */
	private Map<String, Result> results = new HashMap<String, Result>();
	
	/**
	 * Constructor
	 * @param topicPrefix topic prefix
	 */
	public VerifierAggregatorBolt(String topicPrefix) {
		this.TOPIC_PREFIX = topicPrefix;
	}
	
	/**
	 * Prepare function of bolt
	 */
	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}
	
	/**
	 * The output of the {@link VerifierBolt}s are aggregated in this function.
	 * Each bolt provides a timestamp and a verified flag for a given value of the random field.
	 * These are stored and printed to the stdout
	 */
	@Override
	public void execute(Tuple input) {
		boolean verified = input.getBooleanByField("verified");
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		String time = sdf.format(new Date(input.getLongByField("timestamp")));
		String topic = TOPIC_PREFIX + input.getStringByField("random");
		
		if(!results.containsKey(topic)) {
			Result result = new Result(time, verified);
			results.put(topic, result);
		} else {
			results.get(topic).setTime(time);
			results.get(topic).setVerified(verified);
		}
		
		for(Entry<String, Result> entry : results.entrySet()) {
			String not = entry.getValue().isVerified() ? " " : "NOT ";
			System.out.println("Topic " + entry.getKey() + " is " + not + "VERIFIED at " + entry.getValue().getTime());			
		}
		
		collector.ack(input);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

}
