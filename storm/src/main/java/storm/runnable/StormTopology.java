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
//import liveperson.storm.AvroTopology.AvroBolt;
import storm.bolt.AvroDecoderBolt;
import storm.bolt.KafkaBoltWithTimestamp;
import storm.bolt.KafkaSpoutWithTimestamp;
import storm.bolt.SimpleMapper;
import storm.bolt.SimpleTopicSelector;
import storm.bolt.VerifierAggregatorBolt;
import storm.bolt.VerifierBolt;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;
import storm.kafka.bolt.KafkaBolt;

public class StormTopology {

    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {

        final String offsetPath = "";
        final String consumerId = "v1";
        final String sourceTopic = "t";
        final String zookeeper = "localhost:2181";
        final String kafka = "172.17.42.1:9092";
        final String outputTopicNamePrefix = "random_";
        final String randomFieldName = "random";
        final String[] randomValueSet = {"1","2","3"};
        final boolean verify = false;
        final boolean performance = true;
        final boolean local = true;
        
        ZkHosts zkHosts = new ZkHosts(zookeeper);
        
        SpoutConfig kafkaConfig = new SpoutConfig(zkHosts, sourceTopic, offsetPath, consumerId);
        //kafkaConfig.forceFromStart = true;

        KafkaSpout kafkaSpout = createKafkaSpout(kafkaConfig, performance);
        KafkaBolt<String, byte[]> kafkaBolt = createKafkaBolt(outputTopicNamePrefix,performance);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafka-spout", kafkaSpout);
        builder.setBolt("avro-decoder-bolt", new AvroDecoderBolt(performance,randomFieldName)).shuffleGrouping("kafka-spout");
        builder.setBolt("kafka-bolt", kafkaBolt).shuffleGrouping("avro-decoder-bolt");
        
        if(verify) {
        	addVerification(builder, offsetPath, zookeeper, outputTopicNamePrefix, randomFieldName, consumerId, randomValueSet, zkHosts);
        }
        
        //builder.setBolt("kafka-output-bolt", new KafkaOutputBolt(zookeeper, outputTopicNamePrefix))
        //	.shuffleGrouping("avro-decoder-bolt");
        //builder.setBolt("syso-bolt", new SysoBolt()).shuffleGrouping("avro-decoder-bolt");

        Properties props = new Properties();
        props.put("metadata.broker.list", kafka);
        props.put("broker.list", kafka);
        props.put("producer.type", "sync");
        props.put("batch.size", "1");
        props.put("zk.connect", zookeeper); 
        props.put("broker.id", 0); 
        
        Config conf = new Config();
        conf.put(KafkaBolt.KAFKA_BROKER_PROPERTIES, props);
        conf.setDebug(true);
        
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
    
    private static KafkaSpout createKafkaSpout(SpoutConfig kafkaConfig, boolean performance) {
    	KafkaSpout kafkaSpout = null;
    	if(performance) {
        	kafkaSpout = new KafkaSpoutWithTimestamp(kafkaConfig);
        } else {
        	kafkaSpout = new KafkaSpout(kafkaConfig);
        }
    	return kafkaSpout;
    }

    private static KafkaBolt<String, byte[]> createKafkaBolt(String outputTopicNamePrefix, boolean performance) {
    	KafkaBolt<String, byte[]> kafkaBolt = null;
        if(performance) {
        	kafkaBolt = new KafkaBoltWithTimestamp<String, byte[]>();
        } else {
        	kafkaBolt = new KafkaBolt<String, byte[]>();
        }
        
        kafkaBolt.withTopicSelector(new SimpleTopicSelector(outputTopicNamePrefix));
        kafkaBolt.withTupleToKafkaMapper(new SimpleMapper());
    	return kafkaBolt;
    }

	private static void addVerification(TopologyBuilder builder, final String offsetPath, final String zookeeper,
			final String outputTopicNamePrefix, final String randomFieldName, final String consumerId,  final String[] randomValueSet,
			ZkHosts zkHosts) {
		
		BoltDeclarer verifierAggregatorBolt = builder.setBolt("verifier-aggregator-bolt", new VerifierAggregatorBolt(outputTopicNamePrefix));
		for(String value : randomValueSet) {
			String targetTopic = outputTopicNamePrefix + value;
			createTopic(zookeeper, targetTopic);
			SpoutConfig verifierConfig = new SpoutConfig(zkHosts, targetTopic, offsetPath, consumerId+targetTopic);

		    KafkaSpout verifierSpout = new KafkaSpout(verifierConfig);
		    
		    VerifierBolt verifierBolt = new VerifierBolt(value);
		    
			builder.setSpout("verifier-kafka-spout-"+targetTopic, verifierSpout);
			builder.setBolt("verifier-avro-decoder-bolt-"+targetTopic, new AvroDecoderBolt(randomFieldName))
				.shuffleGrouping("verifier-kafka-spout-"+targetTopic);
			builder.setBolt("verifier-bolt-"+targetTopic, verifierBolt)
				.fieldsGrouping("verifier-avro-decoder-bolt-"+targetTopic, new Fields("random"))
				.fieldsGrouping("avro-decoder-bolt", new Fields("random"));
			
			verifierAggregatorBolt.shuffleGrouping("verifier-bolt-"+targetTopic);
		}
	}

	private static void createTopic(final String zookeeper, String targetTopic) {
		try {
			ZkClient zkClient = new ZkClient(zookeeper);
			AdminUtils.createTopic(zkClient, targetTopic, 1, 1, new Properties());
		} catch(TopicExistsException e) {
			
		}
	}

}