package com.rd.main;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;

import com.rd.bolt.YFBolt;
import com.rd.spout.YFSpout;

public class TopologyMain {

	public static void main(String[] args) throws Exception {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("YFS", new YFSpout());
		builder.setBolt("YFB", new YFBolt())
				.shuffleGrouping("YFS");
		
		StormTopology topology = builder.createTopology();
		Config config = new Config();
		config.setDebug(true);
		
		//read from user input later
		config.put("fileToWrite", "C:\\Users\\omsjo\\Desktop\\java\\apache-storm\\output.txt");
		
		LocalCluster cluster = new LocalCluster();
		try {
			cluster.submitTopology("STT", config, topology);
			Thread.sleep(10000);
		}finally {
			cluster.shutdown();
		}
	}

}
