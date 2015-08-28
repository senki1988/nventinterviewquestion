package flink.sink;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import flink.schema.AvroSchema;

public class VerifierSink implements SinkFunction<GenericRecord> {

	/**
	 * Serialization ID
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public void invoke(GenericRecord value) throws Exception {
		//GenericRecord corrected = value.
		//value.put("id", value.get(0));
		//value.put("random", value.get(1));
		//value.put("data", value.get(2));
		System.out.println(value.get(0) + " " + value.get(1) + " " + value.get(2));
	}

}
