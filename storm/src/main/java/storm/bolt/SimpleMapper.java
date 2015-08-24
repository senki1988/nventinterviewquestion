package storm.bolt;

import backtype.storm.tuple.Tuple;
import storm.kafka.bolt.mapper.TupleToKafkaMapper;

public class SimpleMapper implements TupleToKafkaMapper<String, byte[]>{

	private static final long serialVersionUID = 1L;

	@Override
	public String getKeyFromTuple(Tuple tuple) {
		return null;
	}

	@Override
	public byte[] getMessageFromTuple(Tuple tuple) {
		return tuple.getStringByField("message").getBytes();
	}

}
