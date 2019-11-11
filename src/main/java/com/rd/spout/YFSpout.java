package com.rd.spout;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Map;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import yahoofinance.YahooFinance;
import yahoofinance.quotes.stock.StockQuote;

public class YFSpout extends BaseRichSpout {

	private SpoutOutputCollector collector;
	private SimpleDateFormat sdf = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss");

	@Override
	public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void nextTuple() {

		try {
			StockQuote quote = YahooFinance.get("MSFI").getQuote();

			BigDecimal price = quote.getPrice();
			BigDecimal prevClose = quote.getPreviousClose();
			Timestamp timestamp = new Timestamp(System.currentTimeMillis());

			collector.emit(new Values("MSFI", sdf.format(timestamp), price.doubleValue(), prevClose.doubleValue()));
		} catch (Exception e) {

		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("company", "timestamp", "price", "prev_close"));
	}

}
