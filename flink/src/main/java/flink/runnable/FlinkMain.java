package flink.runnable;

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;

import org.I0Itec.zkclient.ZkClient;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.runtime.kryo.Serializers.AvroSchemaSerializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.GroupedDataStream;
import org.apache.flink.streaming.api.datastream.SplitDataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.api.KafkaSink;
import org.apache.flink.streaming.connectors.kafka.api.persistent.PersistentKafkaSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import flink.function.ClonerSelector;
import flink.function.ExtractFieldFlatMapper;
import flink.function.SelectorMapper;
import flink.function.TopicSelector;
import flink.schema.AvroSchema;
import flink.sink.VerifierSink;
import kafka.admin.AdminUtils;
import kafka.common.TopicExistsException;
import kafka.consumer.ConsumerConfig;
import kafka.utils.ZKStringSerializer$;

public class FlinkMain {
	
	private static final Logger LOG = LoggerFactory.getLogger(FlinkMain.class);
	
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
    /**
     * Flag if the cluster is local or remote
     */
    private static boolean local;
	
	public static void main(String[] args) throws Exception {
		
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
		
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.enableCheckpointing(5000);

		DataStream<GenericRecord> sourceTopicStream = env.addSource(createKafkaSource(sourceTopic), sourceTopic);
		SplitDataStream<GenericRecord> splitsForVerification = null;
		if(verify) {
			splitsForVerification = sourceTopicStream.split(new ClonerSelector());
			sourceTopicStream = splitsForVerification.select("first");			
		}
		
		// grouping by random field value
		GroupedDataStream<Tuple2<String, GenericRecord>> groupedStream = sourceTopicStream.flatMap(new ExtractFieldFlatMapper(randomFieldName)).groupBy(0);
		
		TopicSelector topicSelector = new TopicSelector();
		SplitDataStream<Tuple2<String, GenericRecord>> split = groupedStream.split(topicSelector);
		for(String value : randomValueSet) {
			DataStream<Tuple2<String, GenericRecord>> targetTopicStream = split.select(value);
			DataStream<GenericRecord> transformedStream = targetTopicStream.flatMap(new SelectorMapper());
			transformedStream.addSink(createKafkaSink(outputTopicNamePrefix+value));
		}

		if(verify) {
			DataStream<GenericRecord> verifierStream = splitsForVerification.select("second");
			DataStream<GenericRecord> connectedStream = null;
			for(String value : randomValueSet) {
				String targetTopic = outputTopicNamePrefix+value;
				DataStream<GenericRecord> targetTopicSource = env.addSource(createKafkaSource(targetTopic), targetTopic);
				if(connectedStream == null) {
					connectedStream = targetTopicSource;
				} else {
					connectedStream = connectedStream.union(targetTopicSource);
				}
			}
			
			connectedStream.addSink(new VerifierSink());
			//verifierStream.addSink(new VerifierSink());
		}
		
		//System.out.println(env.getExecutionPlan());
		env.execute("Flink stream");
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
		
		return new PersistentKafkaSource<GenericRecord>(topic, new AvroSchema(), consumerConfig);
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
		
		return new KafkaSink<GenericRecord>(kafka, topic, producerProps, new AvroSchema());
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
        local = Boolean.parseBoolean(applicationConfiguration.getProperty("local", "true"));
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
