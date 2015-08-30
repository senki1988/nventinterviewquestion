package nventdata.storm.mapper;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;

import backtype.storm.tuple.Tuple;
import storm.kafka.bolt.mapper.TupleToKafkaMapper;

/**
 * The purpose of this class is to deserialize {@link GenericRecord} to byte array for kafka producer
 * @author senki
 *
 */
public class GenericRecordToKafkaMessageMapper implements TupleToKafkaMapper<String, byte[]> {

	/**
	 * Serialize ID
	 */
	private static final long serialVersionUID = 1L;
	/**
	 * Avro schema
	 */
	private static Schema schema;
	static {
		Schema.Parser parser = new Schema.Parser();
		try {
			schema = parser.parse(GenericRecordToKafkaMessageMapper.class.getResourceAsStream("/kafkatest.avsc"));
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	
	/**
	 * Reusable binary encoder for avro schema serialization
	 */
	private BinaryEncoder binaryEncoder = null;
	
	/**
	 * Key is not used
	 */
	@Override
	public String getKeyFromTuple(Tuple tuple) {
		return null;
	}

	/**
	 * Message from GenericRecord
	 */
	@Override
	public byte[] getMessageFromTuple(Tuple tuple) {
		GenericRecord record = (GenericRecord)tuple.getValueByField("message");
		try {
			DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema);
			ByteArrayOutputStream out = new ByteArrayOutputStream();
			binaryEncoder = EncoderFactory.get().binaryEncoder(out, binaryEncoder);
			writer.write(record, binaryEncoder);
			binaryEncoder.flush();
			return out.toByteArray();
		} catch (IOException e) {
			throw new RuntimeException(e); 
		}
	}

}
