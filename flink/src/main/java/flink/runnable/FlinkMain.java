package flink.runnable;

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;

import org.I0Itec.zkclient.ZkClient;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.GroupedDataStream;
import org.apache.flink.streaming.api.datastream.SplitDataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.api.KafkaSink;
import org.apache.flink.streaming.connectors.kafka.api.persistent.PersistentKafkaSource;

import flink.filter.FilterByValue;
import flink.mapper.ExtractFieldFlatMapper;
import flink.mapper.ExtractMessageMapper;
import flink.performance.PerformanceAvroSchema;
import flink.performance.PerformanceKafkaSink;
import flink.schema.AvroSchema;
import flink.selector.ClonerSelector;
import flink.selector.TopicSelector;
import flink.sink.VerifierSink;
import kafka.admin.AdminUtils;
import kafka.common.TopicExistsException;
import kafka.consumer.ConsumerConfig;
import kafka.utils.ZKStringSerializer$;

public class FlinkMain {
	
	/**
	 * Application configuration
	 */
	private static Properties applicationConfiguration = new Properties();
	
	/**
	 * Path offset to zookeeper root
	 */
    private static String zookeeperRoot;
    /**
     * Kafka consumer id
     */
    private static String consumerId;
    /**
     * Topic that is read
     */
    private static String sourceTopic;
    /**
     * Url to zookeeper
     */
    private static String zookeeper;
    /**
     * Url to kafka
     */
    private static String kafka;
    /**
     * Prefix for the target topics
     */
    private static String outputTopicNamePrefix;
    /**
     * Field name to check in the avro scheme
     */
    private static String randomFieldName;
    /**
     * Possible values for the selected field {@link #randomFieldName}
     */
    private static String[] randomValueSet;
    /**
     * Flag if verification is needed
     */
    private static boolean verify;
    /**
     * Flag if performance test is needed
     */
    private static boolean performance;
	
	public static void main(String[] args) throws Exception {
		
		checkArgs(args);
		
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		//env.enableCheckpointing(5000);

		// kafka message stream
		// kafka message stream
		DataStream<GenericRecord> sourceTopicStream = env.addSource(createKafkaSource(sourceTopic), sourceTopic);
		
		// kafka message is enhanced with the value of random field
		// and grouping by random field value
		GroupedDataStream<Tuple2<String, GenericRecord>> groupedStream = 
				sourceTopicStream.flatMap(new ExtractFieldFlatMapper(randomFieldName)).groupBy(0); 
		
		// based on the random field value, topicSelector splits the stream
		SplitDataStream<Tuple2<String, GenericRecord>> split = groupedStream.split(new TopicSelector());
		for(String value : randomValueSet) {
			// each split is for a value
			DataStream<Tuple2<String, GenericRecord>> targetTopicStream = split.select(value);
			// extracting kafka message
			DataStream<GenericRecord> transformedStream = targetTopicStream.flatMap(new ExtractMessageMapper());
			// send it to the correct topic
			transformedStream.addSink(createKafkaSink(outputTopicNamePrefix+value));
		}
		
		if(verify) {
			addVerification(env, sourceTopicStream);
		}
		
		//System.out.println(env.getExecutionPlan());
		env.execute("Flink stream");
    }

	/**
	 * Checking arguments and setting properties
	 * @param args application arguments
	 */
	private static void checkArgs(String[] args) {
		if(args.length != 1) {
    		throw new RuntimeException("Exactly one parameter is required: the path to the application configuration file");
    	}
    	String applicationConfigurationPath = args[0];
		try {
			applicationConfiguration.loadFromXML(new FileInputStream(new File(applicationConfigurationPath)));
		} catch (Exception e) {
			throw new RuntimeException("Application configuration file not found or cannot be parsed",e);
		}
		
		setProperties();
	}

	/**
	 * Adds verification part to the topology
	 * @param env flink stream execution environment
	 * @param sourceTopicStream source datastream from kafka topic
	 */
	private static void addVerification(final StreamExecutionEnvironment env, DataStream<GenericRecord> sourceTopicStream) {
		// index for the current split
		int splitIndex = 0;
		// splitting the original stream to the number of target topics +1
		SplitDataStream<GenericRecord> splitsForVerification = sourceTopicStream.split(new ClonerSelector(randomValueSet.length+1));
		// first stream is the original
		sourceTopicStream = splitsForVerification.select(String.valueOf(splitIndex));
		// the other splits are assigned to the verifier streams
		for(String value : randomValueSet) {
			String targetTopic = outputTopicNamePrefix+value;
			// get the next split of the stream
			DataStream<GenericRecord> verifierStream = splitsForVerification.select(String.valueOf(++splitIndex));
			// filter by value
			DataStream<GenericRecord> filteredStream = verifierStream.filter(new FilterByValue(value, randomFieldName));
			// reading from the target tooic
			DataStream<GenericRecord> targetTopicSource = env.addSource(createKafkaSource(targetTopic), targetTopic);
			// merge the splitted stream and the target topic stream to a verifier sink
			DataStreamSink<GenericRecord> verifierSink = targetTopicSource.union(filteredStream).addSink(new VerifierSink(value));
			// paralellism is set to 1, to make it easier to handle verification buffer
			verifierSink.setParallelism(1);
		}
	}

	/**
	 * Creates kafka source for the topic {@link #sourceTopic}
	 * @return kafka source
	 */
	private static PersistentKafkaSource<GenericRecord> createKafkaSource(String topic) {
		// creating source topic if it does not exist
		createTopic(topic);
		
		Properties consumerProps = new Properties();
        consumerProps.put("zookeeper.connect", zookeeper+zookeeperRoot); 
        consumerProps.put("group.id", consumerId);
        consumerProps.put("auto.commit.enable", "false");
		ConsumerConfig consumerConfig = new ConsumerConfig(consumerProps);
		return new PersistentKafkaSource<GenericRecord>(topic, performance ? new PerformanceAvroSchema() : new AvroSchema(), consumerConfig);
	}

	/**
	 * Creates kafka sink for the topic that belongs to the value parameter
	 * @param value value of the random field
	 * @return kafka sink for the topic that blongs to the value
	 */
	private static KafkaSink<GenericRecord> createKafkaSink(String topic) {
		Properties producerProps = new Properties();
		producerProps.put("metadata.broker.list", kafka);
		producerProps.put("producer.type", "sync");
		producerProps.put("batch.size", "1");
		producerProps.put("zk.connect", zookeeper+zookeeperRoot); 
		producerProps.put("broker.id", 0); 
		
		if(performance) {
			return new PerformanceKafkaSink<GenericRecord>(kafka, topic, producerProps, new AvroSchema());
		} else {
			return new KafkaSink<GenericRecord>(kafka, topic, producerProps, new AvroSchema());
		}
	}
	
	/**
     * Sets properties based on the given configuration and default values
     */
    private static void setProperties() {
    	// arguments
        zookeeperRoot = applicationConfiguration.getProperty("zookeeper.rootpath", "");
        consumerId = applicationConfiguration.getProperty("kafka.consumerid", "consumer_1");        
        sourceTopic = applicationConfiguration.getProperty("kafka.sourcetopic", "neverwinter");
        zookeeper = applicationConfiguration.getProperty("zookeeper.url", "localhost:2181");
        kafka = applicationConfiguration.getProperty("kafka.url", "localhost:9092");
        outputTopicNamePrefix = applicationConfiguration.getProperty("kafka.targettopicprefix", "random");
        randomFieldName = applicationConfiguration.getProperty("avro.fieldname", "random");
        randomValueSet = applicationConfiguration.getProperty("avro.valueset", "").split(",");
        verify = Boolean.parseBoolean(applicationConfiguration.getProperty("verify", "false"));
        performance = Boolean.parseBoolean(applicationConfiguration.getProperty("performance", "false"));
	}
    
    /**
	 * Helper function to create topic
	 * @param zookeeper zookeeper url
	 * @param targetTopic name of the topic to be created
	 */
	private static void createTopic(String targetTopic) {
		try {
			ZkClient zkClient = new ZkClient(zookeeper, 10000, 10000, ZKStringSerializer$.MODULE$);
			AdminUtils.createTopic(zkClient, targetTopic, 1, 1, new Properties());
		} catch(TopicExistsException e) {
			
		}
	}

}
