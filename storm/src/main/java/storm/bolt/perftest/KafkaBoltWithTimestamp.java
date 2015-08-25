package storm.bolt.perftest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.tuple.Tuple;
import storm.kafka.bolt.KafkaBolt;

/**
 * Wrapper around {@link KafkaBolt} in order to measure performance
 * Time measurement stops at the end of {@link KafkaBolt#execute(Tuple)}
 * @author senki
 *
 * @param <K> Key type
 * @param <V> Value type
 */
public class KafkaBoltWithTimestamp<K,V> extends KafkaBolt<K, V> {
	
	private final static Logger LOGGER = LoggerFactory.getLogger(KafkaBoltWithTimestamp.class);
	/**
	 * Serialization id
	 */
	private static final long serialVersionUID = 1L;
	
	/**
	 * Stores average time of messages
	 */
	private double averageTime = 0;
	/**
	 * Stores average size of messages
	 */
	private long averageSize = 0;
	/**
	 * Stores number of messages
	 */
	private long n = 0;

	/**
	 * At this point the time measurement is stopped. 
	 * The results are printed out to stdout after receiving a message.
	 */
	@Override
	public void execute(Tuple input) {
		super.execute(input);
		long end = System.nanoTime();
		long start = input.getLongByField("timestamp");
		double ms = (end-start)/1000000.0;
		++n;
		
		int messageSize = input.getBinaryByField("message").length;
		
		averageSize = (averageSize*(n-1) + messageSize)/n;
		averageTime = (averageTime*(n-1) + ms)/n;
		
		double averageThroughputRecords = 1000*n/averageTime;
		double averageThroughputBytes = averageSize*averageThroughputRecords;
		
		LOGGER.info(
			"\nAvg throughput of " + n + " messages: {} bytes/second"
			+ "\nAvg throughput of " + n + " messages: {} records/second",
			n, String.valueOf(averageThroughputBytes),
			n, String.valueOf(averageThroughputRecords)
		);
	}
}
