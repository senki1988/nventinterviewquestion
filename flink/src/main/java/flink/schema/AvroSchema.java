package flink.schema;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.streaming.util.serialization.SerializationSchema;

/**
 * The purpose of this class is to deserialize kafka messages into avro {@link GenericRecord}
 * @author senki
 *
 */
public class AvroSchema implements DeserializationSchema<GenericRecord>, SerializationSchema<GenericRecord, byte[]>{

	/**
	 * Serialization ID
	 */
	private static final long serialVersionUID = 1L;
	
	/**
	 * Avro schema
	 */
	/*private static Schema schema;
	static {
		Schema.Parser parser = new Schema.Parser();
		try {
			schema = parser.parse(AvroSchema.class.getResourceAsStream("/kafkatest.avsc"));
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}*/

	private BinaryEncoder binaryEncoder = null;

	/**
	 * Returns the type informaction for {@link GenericRecord}
	 */
	@Override
	public TypeInformation<GenericRecord> getProducedType() {
		return TypeExtractor.getForClass(GenericRecord.class);
	}

	/**
	 * Decodes kafka message by using an avro schema
	 */
	@Override
	public GenericRecord deserialize(byte[] message) {
		Schema.Parser parser = new Schema.Parser();
		Schema schema = null;
		try {
			schema = parser.parse(AvroSchema.class.getResourceAsStream("/kafkatest.avsc"));
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		try {
			DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema);
			Decoder decoder = DecoderFactory.get().binaryDecoder(message, null);
			return reader.read(null, decoder);
		} catch (IOException e) {
			throw new RuntimeException(e); 
		}
	}

	/**
	 * Create kafka message from avro record
	 */
	@Override
	public byte[] serialize(GenericRecord element) {
		Schema.Parser parser = new Schema.Parser();
		Schema schema = null;
		try {
			schema = parser.parse(AvroSchema.class.getResourceAsStream("/kafkatest.avsc"));
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		try {
			DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema);
			ByteArrayOutputStream out = new ByteArrayOutputStream();
			binaryEncoder = EncoderFactory.get().binaryEncoder(out, binaryEncoder);
			writer.write(element, binaryEncoder);
			binaryEncoder.flush();
			return out.toByteArray();
		} catch (IOException e) {
			throw new RuntimeException(e); 
		}
	}
	
	/**
	 * Return true if the next element is the end of the stream.
	 * For {@link AvroSchema}, this is an infinite stream
	 */
	@Override
	public boolean isEndOfStream(GenericRecord nextElement) {
		return false;
	}
}
