package storm.spout.perftest;

import java.util.List;

import backtype.storm.spout.RawMultiScheme;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.performance.PerformanceMeter;

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
	 * This embedded class does the actual wrapping on the scheme to start performance measurement
	 * @author senki
	 *
	 */
	private class ExtendedRawScheme extends RawMultiScheme {
		/**
		 * Serialization id
		 */
		private static final long serialVersionUID = 1L;

		/**
		 * Start measurement after deserializing message
		 */
		@Override
		public Iterable<List<Object>> deserialize(byte[] ser) {
			PerformanceMeter.messageRead();
			Iterable<List<Object>> ret = super.deserialize(ser);
			return ret;
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
}
