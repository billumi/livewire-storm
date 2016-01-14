package chenxi.livewire.storm.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import chenxi.livewire.yahoo.Stock;
import chenxi.livewire.yahoo.YahooFinance;


import java.io.IOException;
import java.math.BigDecimal;
import java.util.Map;

/**
 * livewire-storm
 * <p>
 * Created by chenxili on 06/01/2016.
 */
public class YahooFinanceSpout extends BaseRichSpout{

    private SpoutOutputCollector spoutOutputCollector;

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream("", new Fields("message"));
    }

    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
       this.spoutOutputCollector = spoutOutputCollector;
    }

    public void nextTuple() {

        Stock stock = null;

        try {
            stock = YahooFinance.get("INTC");
        } catch (IOException e) {
            e.printStackTrace();
        }

        BigDecimal price = stock.getQuote().getPrice();
        BigDecimal change = stock.getQuote().getChangeInPercent();
        BigDecimal peg = stock.getStats().getPeg();
        BigDecimal dividend = stock.getDividend().getAnnualYieldPercent();

        stock.print();

        spoutOutputCollector.emit(null);

    }
}
