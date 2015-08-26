package flink.runnable;

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;

import org.I0Itec.zkclient.ZkClient;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SplitDataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.api.KafkaSink;
import org.apache.flink.streaming.connectors.kafka.api.persistent.PersistentKafkaSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import flink.function.ExtractFieldFlatMapper;
import flink.function.TopicSelector;
import flink.schema.AvroSchema;
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
		
		// creating source topic if it does not exist
		createTopic(sourceTopic);
		
		
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.enableCheckpointing(5000);

		Properties props = new Properties();
        props.put("zookeeper.connect", zookeeper); 
        props.put("group.id", "consumer_flink");
        props.put("auto.commit.enable", "false");
		ConsumerConfig consumerConfig = new ConsumerConfig(props);
		
		ExtractFieldFlatMapper flatMapper = new ExtractFieldFlatMapper(randomFieldName);
		DataStream<GenericRecord> stream = env
				.addSource(new PersistentKafkaSource<GenericRecord>(sourceTopic, new AvroSchema(), consumerConfig));
		stream.flatMap(flatMapper).groupBy(0); // grouping by random field value
		
		TopicSelector topicSelector = new TopicSelector(randomFieldName);
		SplitDataStream<GenericRecord> split = stream.split(topicSelector);
		for(String value : randomValueSet) {
			DataStream<GenericRecord> targetTopicStream = split.select(value);
			targetTopicStream.addSink(new KafkaSink<GenericRecord>(zookeeper, outputTopicNamePrefix+value, new AvroSchema()));
		}
		
		System.out.println(env.getExecutionPlan());
		
		//env.execute("Flink stream");
		
		
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
