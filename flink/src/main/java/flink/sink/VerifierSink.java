package flink.sink;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;

/**
 * The purpose of this class is to verify results by 
 * matching records from the source topic and the given target topic
 * @author senki
 *
 */
public class VerifierSink implements SinkFunction<byte[]> {

	/**
	 * Serialization ID
	 */
	private static final long serialVersionUID = 1L;

	/**
	 * Stores the messages that are arrived from the source topic. This is a
	 * buffer.
	 */
	private List<String> buffer = new LinkedList<String>();
	
	/**
	 * Stores the topic name
	 */
	private final String TOPIC;
	
	/**
	 * Constructor
	 * @param topicPrefix topic prefix
	 */
	public VerifierSink(String topic) {
		this.TOPIC = topic;
	}

	/**
	 * Handles buffer and creates report
	 */
	@Override
	public void invoke(byte[] value) throws Exception {	
		// string conversion to be able to use ".contains()"
		String valueString = new String(value);
		
		if (buffer.contains(valueString)) {
			buffer.remove(valueString);
		} else {
			buffer.add(valueString);
		}
		
		boolean verified = buffer.isEmpty();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		String time = sdf.format(new Date(System.currentTimeMillis()));
		String not = verified ? "" : "NOT ";
		
		System.out.println("Topic " + TOPIC + " is " + not + "VERIFIED at " + time);
		
	}
}
