package storm.bolt.notused;

import java.util.Map;
import java.util.Properties;

import org.I0Itec.zkclient.ZkClient;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import kafka.admin.AdminUtils;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class KafkaOutputBolt extends BaseRichBolt {
    private static final long serialVersionUID = 1L;

    private String brokerConnectString;
    private String serializerClass;
    private final String TOPIC_PREFIX;
    
    private transient Producer<String, String> producer;
    private transient OutputCollector collector;

    public KafkaOutputBolt(String brokerConnectString, String topicPrefix) {
    	TOPIC_PREFIX = topicPrefix;
        this.serializerClass = "kafka.serializer.StringEncoder";
        this.brokerConnectString = brokerConnectString;
    }

    @Override
    public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector) {
        Properties props = new Properties();
        props.put("metadata.broker.list", brokerConnectString);
        props.put("serializer.class", serializerClass);
        props.put("producer.type", "sync");
        props.put("batch.size", "1");

        ProducerConfig config = new ProducerConfig(props);
        producer = new Producer<String, String>(config);

        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
    	String topic = TOPIC_PREFIX + input.getStringByField("random");
        String kafkaMessage = null;
        try {
            kafkaMessage = "cica";//input.getStringByField("payload");

            KeyedMessage<String, String> data = new KeyedMessage<String, String>(topic, kafkaMessage);
            System.out.println("Sending data to " + topic + ": " + kafkaMessage);
            try {
            	producer.send(data);
            } catch(Exception e) { 
            	ZkClient zookeeperClient = new ZkClient(brokerConnectString);
				AdminUtils.createTopic(zookeeperClient, topic, 1, 1, new Properties());
            	producer.send(data);
            }
            System.out.println("Data sent");
            collector.ack(input);
        } catch (Exception e) {
            collector.fail(input);
            throw new RuntimeException("Error occurred during processing and sending message to kafka", e);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
