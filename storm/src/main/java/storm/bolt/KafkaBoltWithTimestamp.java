package storm.bolt;

import backtype.storm.tuple.Tuple;
import storm.kafka.bolt.KafkaBolt;

public class KafkaBoltWithTimestamp<K,V> extends KafkaBolt<K, V> {
	private static final long serialVersionUID = 1L;
	
	private double averageTime = 0;
	private long averageSize = 0;
	private long n = 0;

	@Override
	public void execute(Tuple input) {
		super.execute(input);
		long end = System.nanoTime();
		long start = input.getLongByField("timestamp");
		double ms = (end-start)/1000000.0;
		++n;
		
		int messageSize = input.getStringByField("message").getBytes().length;
		
		averageSize = (averageSize*(n-1) + messageSize)/n;
		averageTime = (averageTime*(n-1) + ms)/n;
		
		double averageThroughputRecords = 1000*n/averageTime;
		double averageThroughputBytes = averageSize*averageThroughputRecords;
		
		System.out.println(
			//"======= Elapsed time (ms): " + String.valueOf(ms) 
			//+ "\nAvg of " + n + " messages: " + String.valueOf(averageTime)
			//+ "\nAvg size of " + n + " messages: " + String.valueOf(averageSize) + " bytes"
			//+ 
			"\nAvg throughput of " + n + " messages: " + String.valueOf(averageThroughputBytes) + " bytes/second"
			+ "\nAvg throughput of " + n + " messages: " + String.valueOf(averageThroughputRecords) + " records/second"
		);
	}
}
