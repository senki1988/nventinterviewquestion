package storm.bolt;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import backtype.storm.spout.RawMultiScheme;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;

public class KafkaSpoutWithTimestamp extends KafkaSpout {
	
	private static final long serialVersionUID = 1L;

	private long timestamp;
	
	public class ExtendedRawScheme extends RawMultiScheme {
		private static final long serialVersionUID = 1L;

		@Override
		public Iterable<List<Object>> deserialize(byte[] ser) {
			Iterable it = super.deserialize(ser);
			Iterator iterator = it.iterator();
			List<Object> list = new ArrayList<Object>();
			
			while(iterator.hasNext()) {
				Object next = iterator.next();
				if(next instanceof List) {
					for(Object o : (List)next) {
						list.add(o);
					}
				}
			}
			
			list.add(timestamp);
			return Arrays.asList(list);
		}
		@Override
		public Fields getOutputFields() {
			Fields fields = super.getOutputFields();
			List<String> fieldList = fields.toList();
			fieldList.add("timestamp");
			return new Fields(fieldList);
		}
	}

	public KafkaSpoutWithTimestamp(SpoutConfig spoutConf) {
		super(spoutConf);
		spoutConf.scheme = new ExtendedRawScheme();
	}
	
	@Override
	public void nextTuple() {
		timestamp = System.nanoTime();
		super.nextTuple();
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		super.declareOutputFields(declarer);
	}

}
