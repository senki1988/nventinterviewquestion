package storm.runnable;

import java.util.Properties;

import org.I0Itec.zkclient.ZkClient;

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
import storm.bolt.AvroDecoderBolt;
import storm.bolt.KafkaBoltWithTimestamp;
import storm.bolt.KafkaSpoutWithTimestamp;
import storm.bolt.SimpleTopicSelector;
import storm.bolt.VerifierAggregatorBolt;
import storm.bolt.VerifierBolt;
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

    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
    	// TODO
    	// arguments
        final String offsetPath = "";
        final String consumerId = "v1";
        final String sourceTopic = "t";
        final String zookeeper = "localhost:2181";
        final String kafka = "172.17.42.1:9092";
        final String outputTopicNamePrefix = "random_";
        final String randomFieldName = "random";
        final String[] randomValueSet = {"1","2","3"};
        final boolean verify = true;
        final boolean performance = false;
        final boolean local = false;
        
        // zookeeper hosts
        ZkHosts zkHosts = new ZkHosts(zookeeper);
        // configuration for kafka
        SpoutConfig kafkaConfig = new SpoutConfig(zkHosts, sourceTopic, offsetPath, consumerId);
        //kafkaConfig.forceFromStart = true;

        // kafka spout and bolt to read and write from/to kafka
        KafkaSpout kafkaSpout = createKafkaSpout(kafkaConfig, performance);
        KafkaBolt<String, byte[]> kafkaBolt = createKafkaBolt(outputTopicNamePrefix, randomFieldName, performance);

        // simple topology: kafka -> avro decoder bolt -> kafka
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafka-spout", kafkaSpout);
        builder.setBolt("avro-decoder-bolt", new AvroDecoderBolt(randomFieldName, performance)).shuffleGrouping("kafka-spout");
        builder.setBolt("kafka-bolt", kafkaBolt).shuffleGrouping("avro-decoder-bolt");
        
        // if verification is needed, topology is extended
        if(verify) {
        	addVerification(builder, offsetPath, zookeeper, outputTopicNamePrefix, randomFieldName, consumerId, randomValueSet, zkHosts);
        }
        
        //builder.setBolt("kafka-output-bolt", new KafkaOutputBolt(zookeeper, outputTopicNamePrefix))
        //	.shuffleGrouping("avro-decoder-bolt");
        //builder.setBolt("syso-bolt", new SysoBolt()).shuffleGrouping("avro-decoder-bolt");

        // properties for kafka
        Properties props = new Properties();
        props.put("metadata.broker.list", kafka);
        props.put("broker.list", kafka);
        props.put("producer.type", "sync");
        props.put("batch.size", "1");
        props.put("zk.connect", zookeeper); 
        props.put("broker.id", 0); 
        
        // storm config
        Config conf = new Config();
        conf.put(KafkaBolt.KAFKA_BROKER_PROPERTIES, props);
        conf.setDebug(true);
        
        // local or remote cluster
        if(local) {
	        LocalCluster cluster = new LocalCluster();
	        cluster.submitTopology("stormTopology", conf, builder.createTopology());
	        Utils.sleep(100000);
	        cluster.killTopology("stormTopology");
	        cluster.shutdown();
        } else {
        	conf.setNumWorkers(20);
        	conf.setMaxSpoutPending(5000);
			StormSubmitter.submitTopology("stormTopology", conf, builder.createTopology());
        }
       
    }
    
    /**
     * Selects {@link KafkaSpout} implementation based on the performance flag 
     * @param kafkaConfig kafka configuration
     * @param performance performance test is needed?
     * @return instance of KafkaSpout with or without performance measurement wrapper
     */
    private static KafkaSpout createKafkaSpout(SpoutConfig kafkaConfig, boolean performance) {
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
     * @param offsetPath root path in zookeeper
     * @param zookeeper zookeeper url
     * @param zkHosts zookeeper hosts
     * @param outputTopicNamePrefix topic name prefix for the target topics
     * @param randomFieldName field name to check in the avro scheme
     * @param consumerId name of the consumer
     * @param randomValueSet the possible values of the random field
     */
	private static void addVerification(TopologyBuilder builder, final String offsetPath, final String zookeeper,
			final String outputTopicNamePrefix, final String randomFieldName, final String consumerId,  final String[] randomValueSet,
			ZkHosts zkHosts) {
		// creating aggregator bolt to collect output of verifier bolts
		BoltDeclarer verifierAggregatorBolt = builder.setBolt("verifier-aggregator-bolt", new VerifierAggregatorBolt(outputTopicNamePrefix));
		// looping through the valueset to create the verifier bolts for them
		for(String value : randomValueSet) {
			// creating target topic
			String targetTopic = outputTopicNamePrefix + value;
			createTopic(zookeeper, targetTopic);
			// creating KafkaSpout for the target topic
			SpoutConfig verifierConfig = new SpoutConfig(zkHosts, targetTopic, offsetPath, consumerId+targetTopic);
		    KafkaSpout verifierSpout = new KafkaSpout(verifierConfig);
		    // Verifier bolt for the target topic
		    VerifierBolt verifierBolt = new VerifierBolt(value);
		    
		    // adding spout of the target topic to the topology
			builder.setSpout("verifier-kafka-spout-"+targetTopic, verifierSpout);
			// connecting an AvroDecoderBolt to it
			builder.setBolt("verifier-avro-decoder-bolt-"+targetTopic, new AvroDecoderBolt(randomFieldName))
				.shuffleGrouping("verifier-kafka-spout-"+targetTopic);
			// connecting VerifierBolt to it, that receives messages both from the source and target topics
			builder.setBolt("verifier-bolt-"+targetTopic, verifierBolt)
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
	private static void createTopic(final String zookeeper, String targetTopic) {
		try {
			ZkClient zkClient = new ZkClient(zookeeper);
			AdminUtils.createTopic(zkClient, targetTopic, 1, 1, new Properties());
		} catch(TopicExistsException e) {
			
		}
	}

}