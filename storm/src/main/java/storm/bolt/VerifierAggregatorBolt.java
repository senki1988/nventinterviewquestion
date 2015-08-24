package storm.bolt;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class VerifierAggregatorBolt extends BaseRichBolt {
	
	private static final long serialVersionUID = 1L;

	private OutputCollector collector;
	
	private final String TOPIC_PREFIX;
	
	private class Result {
		private String time;
		private boolean verified;
		
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
	
	private Map<String, Result> results = new HashMap<String, Result>();
	
	public VerifierAggregatorBolt(String topicPrefix) {
		this.TOPIC_PREFIX = topicPrefix;
	}
	
	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}
	
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
