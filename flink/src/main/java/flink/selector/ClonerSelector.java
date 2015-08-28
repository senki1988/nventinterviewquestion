package flink.selector;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.streaming.api.collector.selector.OutputSelector;

/**
 * Splits kafka message stream
 * @author senki
 *
 */
public class ClonerSelector implements OutputSelector<byte[]> {

	/**
	 * Serialization ID
	 */
	private static final long serialVersionUID = 1L;
	
	/**
	 * Number of splits at the output
	 */
	private int numberOfSplits;
	
	public ClonerSelector(int numberOfSplits) {
		this.numberOfSplits = numberOfSplits;
	}
	
	/**
	 * Splits stream to {@link #numberOfSplits} piece
	 */
	@Override
	public Iterable<String> select(byte[] value) {
		
		List<String> list = new ArrayList<String>(numberOfSplits);
		for(int i=0; i<numberOfSplits; ++i) {
			list.add(String.valueOf(i));
		}
		
		return list;
	}

}
