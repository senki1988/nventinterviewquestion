package storm.bolt;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import backtype.storm.spout.RawMultiScheme;
import backtype.storm.tuple.Fields;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;

/**
 * Wrapper around {@link KafkaSpout} in order to measure performance
 * Time measurement starts at the beginning of {@link KafkaSpout#nextTuple()}
 * @author senki
 *
 */
public class KafkaSpoutWithTimestamp extends KafkaSpout {
	
	/**
	 * Serialization id
	 */
	private static final long serialVersionUID = 1L;

	/**
	 * Timestamp stores the time when {@link KafkaSpoutWithTimestamp#nextTuple()} is called
	 */
	private long timestamp;
	
	/**
	 * This embedded class does the actual wrapping on the scheme. 
	 * It adds a new field to the existing one from the RawMultiScheme
	 * @author senki
	 *
	 */
	private class ExtendedRawScheme extends RawMultiScheme {
		/**
		 * Serialization id
		 */
		private static final long serialVersionUID = 1L;

		/**
		 * Returns the list of tuples. This contains the value for timestamp too.
		 */
		@SuppressWarnings("rawtypes")
		@Override
		public Iterable<List<Object>> deserialize(byte[] ser) {
			// getting tuples from the super implementation
			Iterable it = super.deserialize(ser);
			Iterator iterator = it.iterator();
			List<Object> list = new ArrayList<Object>();
			// adding the already existing objects to the new list
			while(iterator.hasNext()) {
				Object next = iterator.next();
				if(next instanceof List) {
					for(Object o : (List)next) {
						list.add(o);
					}
				}
			}
			// finally adding timestamp
			list.add(timestamp);
			return Arrays.asList(list);
		}
		/**
		 * Name of the output fields. The original field list is extended with a new "timestamp" field.
		 */
		@Override
		public Fields getOutputFields() {
			Fields fields = super.getOutputFields();
			List<String> fieldList = fields.toList();
			fieldList.add("timestamp");
			return new Fields(fieldList);
		}
	}

	/**
	 * Constructor, sets scheme to {@link ExtendedRawScheme}
	 * @param spoutConf configuration for KafkaSpout
	 * @see KafkaSpout#KafkaSpout(SpoutConfig)
	 */
	public KafkaSpoutWithTimestamp(SpoutConfig spoutConf) {
		super(spoutConf);
		spoutConf.scheme = new ExtendedRawScheme();
	}
	
	/**
	 * Starts time measurement and calls the super implementation.
	 * @see KafkaSpout#nextTuple()
	 */
	@Override
	public void nextTuple() {
		timestamp = System.nanoTime();
		super.nextTuple();
	}
}
