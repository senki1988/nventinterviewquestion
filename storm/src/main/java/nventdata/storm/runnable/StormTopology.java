package nventdata.storm.runnable;

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;

import org.I0Itec.zkclient.ZkClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import kafka.admin.AdminUtils;
import kafka.common.TopicExistsException;
import kafka.utils.ZKStringSerializer$;
import nventdata.storm.bolt.AvroDecoderBolt;
import nventdata.storm.bolt.SimpleTopicSelector;
import nventdata.storm.bolt.VerifierAggregatorBolt;
import nventdata.storm.bolt.VerifierBolt;
import nventdata.storm.performance.KafkaBoltWithTimestamp;
import nventdata.storm.performance.KafkaSpoutWithTimestamp;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;
import storm.kafka.bolt.KafkaBolt;

/**
 * The purpose of this class is to built up a topology and run it on a local or remote storm cluster
 * @author senki
 *
 */
public class StormTopology {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(StormTopology.class);
	
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
    /**
     * Number of workers
     */
    private static int parallelism = 4;
    

    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
    	checkArgs(args);
		
		// creating source topic if it does not exist
		createTopic(sourceTopic);
    	
        // zookeeper hosts
        ZkHosts zkHosts = new ZkHosts(zookeeper);
        // configuration for kafka
        SpoutConfig kafkaConfig = new SpoutConfig(zkHosts, sourceTopic, zookeeperRoot, consumerId);
        //kafkaConfig.forceFromStart = true;

        // kafka spout and bolt to read and write from/to kafka
        KafkaSpout kafkaSpout = createKafkaSpout(kafkaConfig, performance);
        KafkaBolt<String, byte[]> kafkaBolt = createKafkaBolt(outputTopicNamePrefix, randomFieldName, performance);

        // simple topology: kafka -> avro decoder bolt -> kafka
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafka-spout", kafkaSpout, parallelism);
        builder.setBolt("avro-decoder-bolt", new AvroDecoderBolt(randomFieldName), parallelism).shuffleGrouping("kafka-spout");
        builder.setBolt("kafka-bolt", kafkaBolt, parallelism).shuffleGrouping("avro-decoder-bolt");
        
        // if verification is needed, topology is extended
        if(verify) {
        	addVerification(builder, zkHosts);
        }

        // properties for kafka
        Properties props = new Properties();
        props.put("metadata.broker.list", kafka);
        props.put("producer.type", "sync");
        props.put("batch.size", "1");
        props.put("zk.connect", zookeeper); 
        props.put("broker.id", 0); 
        
        // storm config
        Config conf = new Config();
        //conf.setNumWorkers(parallelism);
        //conf.setNumWorkers(4);
        conf.put(KafkaBolt.KAFKA_BROKER_PROPERTIES, props);
        conf.setDebug(true);
        
        // local or remote cluster
        if(local) {
	        LocalCluster cluster = new LocalCluster();
	        cluster.submitTopology("stormTopology", conf, builder.createTopology());
	        Utils.sleep(100000000);
	        cluster.killTopology("stormTopology");
	        cluster.shutdown();
        } else {
        	conf.setNumWorkers(parallelism);
        	conf.setMaxSpoutPending(5000);
			StormSubmitter.submitTopology("stormTopology", conf, builder.createTopology());
        }
       
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
        
        LOGGER.info("The following properties are set: \n" 
    			+ "Zookeeper root: {}\n"
    			+ "Zookeeper url: {}\n"
    			+ "Kafka url: {}\n"
    			+ "Kafka consumer id: {}\n"
    			+ "Kafka source topic name: {}\n"
    			+ "Kafka target topic name prefix: {}\n"
    			+ "Avro field name: {}\n"
    			+ "Avro valueset: {}\n"
    			+ "Verification enabled: {}\n"
    			+ "Performance mesaurement enabled: {}\n"
    			+ "Local enabled: {}\n",
    			zookeeperRoot,
    			zookeeper,
    			kafka,
    			consumerId,
    			sourceTopic,
    			outputTopicNamePrefix,
    			randomFieldName,
    			randomValueSet,
    			verify,
    			performance,
    			local
    		);
        
	}

	/**
     * Selects {@link KafkaSpout} implementation based on the performance flag 
     * @param kafkaConfig kafka configuration
     * @param performance performance test is needed?
     * @return instance of KafkaSpout with or without performance measurement wrapper
     */
    private static KafkaSpout createKafkaSpout(SpoutConfig kafkaConfig, boolean performance) {
    	LOGGER.info("Creating KafkaSpout for topic {}", sourceTopic);
    	KafkaSpout kafkaSpout = null;
    	if(performance) {
        	kafkaSpout = new KafkaSpoutWithTimestamp(kafkaConfig);
        } else {
        	kafkaSpout = new KafkaSpout(kafkaConfig);
        }
    	return kafkaSpout;
    }

    /**
     * Selects {@link KafkaBolt} implementation based on the performance flag 
     * @param outputTopicNamePrefix topic name prefix for the target topics
     * @param randomFieldName field name to check in the avro scheme
     * @param performance performance test is needed?
     * @return instance of KafkaBolt with or without performance measurement wrapper
     */
    private static KafkaBolt<String, byte[]> createKafkaBolt(String outputTopicNamePrefix, String randomFieldName, boolean performance) {
    	KafkaBolt<String, byte[]> kafkaBolt = null;
        if(performance) {
        	kafkaBolt = new KafkaBoltWithTimestamp<String, byte[]>();
        } else {
        	kafkaBolt = new KafkaBolt<String, byte[]>();
        }
        
        kafkaBolt.withTopicSelector(new SimpleTopicSelector(outputTopicNamePrefix));
    	return kafkaBolt;
    }

    /**
     * Extends topology with the verification part
     * @param builder topology builder to extend
     * @param zookeeperRoot root path in zookeeper
     * @param zookeeper zookeeper url
     * @param zkHosts zookeeper hosts
     * @param outputTopicNamePrefix topic name prefix for the target topics
     * @param randomFieldName field name to check in the avro scheme
     * @param consumerId name of the consumer
     * @param randomValueSet the possible values of the random field
     */
	private static void addVerification(TopologyBuilder builder, ZkHosts zkHosts) {
		LOGGER.info("Adding verification part");
		// creating aggregator bolt to collect output of verifier bolts
		BoltDeclarer verifierAggregatorBolt = builder.setBolt("verifier-aggregator-bolt", new VerifierAggregatorBolt(outputTopicNamePrefix), 1);
		// looping through the valueset to create the verifier bolts for them
		for(String value : randomValueSet) {
			// creating target topic
			String targetTopic = outputTopicNamePrefix + value;
			createTopic(targetTopic);
			// creating KafkaSpout for the target topic
			SpoutConfig verifierConfig = new SpoutConfig(zkHosts, targetTopic, zookeeperRoot, consumerId+targetTopic);
		    KafkaSpout verifierSpout = new KafkaSpout(verifierConfig);
		    // Verifier bolt for the target topic
		    VerifierBolt verifierBolt = new VerifierBolt(value);
		    
		    // adding spout of the target topic to the topology
			builder.setSpout("verifier-kafka-spout-"+targetTopic, verifierSpout, 1);
			// connecting an AvroDecoderBolt to it
			builder.setBolt("verifier-avro-decoder-bolt-"+targetTopic, new AvroDecoderBolt(randomFieldName), 1)
				.shuffleGrouping("verifier-kafka-spout-"+targetTopic);
			// connecting VerifierBolt to it, that receives messages both from the source and target topics
			builder.setBolt("verifier-bolt-"+targetTopic, verifierBolt, 1)
				.fieldsGrouping("verifier-avro-decoder-bolt-"+targetTopic, new Fields("random"))
				.fieldsGrouping("avro-decoder-bolt", new Fields("random"));
			// finally connecting the VerifierBolt to the common aggregator bolt
			verifierAggregatorBolt.shuffleGrouping("verifier-bolt-"+targetTopic);
		}
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
			LOGGER.info("Topic {} is created.", targetTopic);
		} catch(TopicExistsException e) {
			LOGGER.debug("Topic {} already exists", targetTopic);
		}
	}

}