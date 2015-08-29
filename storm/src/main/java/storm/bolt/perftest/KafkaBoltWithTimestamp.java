package storm.bolt.perftest;

import backtype.storm.tuple.Tuple;
import storm.kafka.bolt.KafkaBolt;
import storm.performance.PerformanceMeter;

/**
 * Wrapper around {@link KafkaBolt} in order to measure performance
 * Time measurement stops at the end of {@link KafkaBolt#execute(Tuple)}
 * @author senki
 *
 * @param <K> Key type
 * @param <V> Value type
 */
public class KafkaBoltWithTimestamp<K,V> extends KafkaBolt<K, V> {
	
	/**
	 * Serialization id
	 */
	private static final long serialVersionUID = 1L;

	/**
	 * At this point the time measurement is stopped.
	 */
	@Override
	public void execute(Tuple input) {
		super.execute(input);
		PerformanceMeter.messageWritten(input.getBinaryByField("message").length);
	}
}
