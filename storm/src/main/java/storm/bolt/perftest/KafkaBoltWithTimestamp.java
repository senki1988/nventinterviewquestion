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
	 * After this time, the performance measurement resets. (in ns)
	 */
	private static final long MAX_WAITING_TIME_BEFORE_RESET = 30*1000*1000*1000;
	
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
	 * Stores the earliest message to calculate the total elapsed time
	 */
	private Long earliestMessage = null;
	/**
	 * Stores the latest message to check performance reset
	 */
	private Long latestMessage = null; 

	/**
	 * At this point the time measurement is stopped. 
	 * The results are printed out to stdout after receiving a message.
	 */
	@Override
	public void execute(Tuple input) {
		super.execute(input);
		long end = System.nanoTime();
		long start = input.getLongByField("timestamp");
		if(earliestMessage == null || start<earliestMessage) {
			earliestMessage = start;
		}
		
		// if waited for too long, resetting measurement
		if(latestMessage != null && start-latestMessage>MAX_WAITING_TIME_BEFORE_RESET) {
			earliestMessage=start;
			latestMessage=end;
			n=0;
			averageSize=0;
			averageTime=0;
		}
		latestMessage = end;
		
		double globalTime = (end-earliestMessage)/1000000.0;
		double messageTime = (end-start)/1000000.0;
		++n;
		
		int messageSize = input.getBinaryByField("message").length;
		
		averageSize = (averageSize*(n-1) + messageSize)/n;
		averageTime = (averageTime*(n-1) + messageTime)/n;
		
		//double averageThroughputRecords = 1000*n/averageTime;
		double averageThroughputRecords = 1000*n/globalTime;
		double averageThroughputBytes = averageSize*averageThroughputRecords;
		
		LOGGER.info(
			  "\nAvg single message time based on {} messages: {} bytes/second"
			+ "\nAvg throughput of {} messages: {} bytes/second"
			+ "\nAvg throughput of {} messages: {} records/second",
			n, averageTime,
			n, averageThroughputBytes,
			n, averageThroughputRecords
		);
	}
}
