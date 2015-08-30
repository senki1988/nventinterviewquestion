package storm.performance;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The purpose of this class is to store indicators for performance measurement
 * @author senki
 *
 */
public class PerformanceMeter {
	
	/**
	 * Logger
	 */
	private static final Logger LOGGER = LoggerFactory.getLogger(PerformanceMeter.class);
	/**
	 * Number of read messages
	 */
	private static int numberOfReadMessages = 0;
	/**
	 * Number of written messages
	 */
	private static int numberOfWrittenMessages = 0;
	/**
	 * Stores average size of messages
	 */
	private static long averageSize = 0;
	/**
	 * Stores the earliest message to calculate the total elapsed time
	 */
	private static Long earliestMessage = null;
	/**
	 * Stores the latest message to check performance reset
	 */
	private static Long latestMessage = null; 
	/**
	 * After this time, the performance measurement resets. (in ns)
	 */
	private static final long MAX_WAITING_TIME_BEFORE_RESET = 30*1000*1000*1000l;
	
	/**
	 * Increases the value of {@link #numberOfReadMessages} and saves the time of the first message
	 */
	public static synchronized void messageRead() {
		if(earliestMessage == null) {
			earliestMessage = System.nanoTime();
		}
		++numberOfReadMessages;
	}
	
	/**
	 * Increases the size of {@link #numberOfWrittenMessages} and creates a report if the
	 * number read and written messages are equal
	 * @param messageSize size of the written message
	 */
	public static synchronized void messageWritten(final int messageSize) {
		if(latestMessage != null && System.nanoTime()-latestMessage > MAX_WAITING_TIME_BEFORE_RESET) {
			earliestMessage=null;
			numberOfReadMessages -= numberOfWrittenMessages;
			numberOfWrittenMessages = 0;
		}
		++numberOfWrittenMessages;
		latestMessage = System.nanoTime();
		
		averageSize = (averageSize*(numberOfWrittenMessages-1) + messageSize)/numberOfWrittenMessages;
		
		if(numberOfWrittenMessages == numberOfReadMessages) {
			double averageThroughputRecords = 1000*numberOfWrittenMessages/((latestMessage-earliestMessage)/1000000.0);
			double averageThroughputBytes = averageSize*averageThroughputRecords;
			
			LOGGER.info(
				   "\nAvg throughput of {} messages: {} bytes/second"
				+ "\nAvg throughput of {} messages: {} records/second",
				numberOfWrittenMessages, averageThroughputBytes,
				numberOfWrittenMessages, averageThroughputRecords
			);
		}
		
	}
	
}
