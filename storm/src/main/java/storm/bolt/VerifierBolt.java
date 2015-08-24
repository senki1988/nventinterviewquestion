package storm.bolt;

import java.util.ArrayList;
import java.util.GregorianCalendar;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class VerifierBolt extends BaseRichBolt {
	
	private static final long serialVersionUID = 1L;

	private OutputCollector collector;
	
	private List<String> fromSource = new LinkedList<String>();
	private List<String> fromTarget = new LinkedList<String>();
	
	private final String value;
	
	public VerifierBolt(String value) {
		this.value = value;
	}
	
	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	protected String getMessage(String b) {    
	    StringBuilder sb = new StringBuilder();
	    
	    for(byte singleByte: b.getBytes()) {
	    	sb.append(String.format("%02x", singleByte));
	    }
	    
	    return sb.toString();
	}

	@Override
	public void execute(Tuple input) {
		if(!value.equals(input.getStringByField("random"))) {
			collector.ack(input);
			return;
		}
		if(input.getSourceComponent().startsWith("verifier")) {
			handleLists(fromTarget, fromSource, input.getStringByField("message"));
		} else {
			handleLists(fromSource, fromTarget, input.getStringByField("message"));
		}
		
		List<Object> result = new ArrayList<Object>(3);
		result.add(value);
		result.add((new GregorianCalendar()).getTimeInMillis());
		result.add(fromSource.isEmpty() && fromTarget.isEmpty());
		collector.emit(result);

        collector.ack(input);
	}

	private void handleLists(List<String> addTo, List<String> removeFrom, String message) {
		if(removeFrom.contains(message)) {
			removeFrom.remove(message);
		} else {
			addTo.add(message);
		}
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("random","timestamp","verified"));
	}

}
