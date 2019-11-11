package com.rd.bolt;

import java.io.PrintWriter;
import java.util.Map;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class YFBolt extends BaseBasicBolt {
	
	private PrintWriter writer;
	
	@Override
	public void prepare(Map<String, Object> topoConf, TopologyContext context) {
		String fileName = topoConf.get("fileToWrite").toString();
		
		try {
			this.writer = new PrintWriter(fileName, "UTF-8");
		}catch(Exception e) {
			throw new RuntimeException("Error opening file ["+fileName+"]");
		}
	}

	@Override
	public void cleanup() {
		super.cleanup();
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {

		String symbol = input.getValue(0).toString();
		String timestamp = input.getString(1);
		
		Double price = (Double) input.getValueByField("price");
		Double prevClose = (Double) input.getValueByField("prev_close");
		
		Boolean gain = true;
		
		if(price<=prevClose) {
			gain = false;
		}
		
		collector.emit(new Values(symbol, timestamp, price, gain));
		writer.println(symbol+","+timestamp+","+price+","+gain);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("company", "timestamp", "price", "gain"));
	}

}
