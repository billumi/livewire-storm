package chenxi.livewire.storm.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

import java.util.Map;

/**
 * livewire-storm
 *
 * Created by chenxili on 06/01/2016.
 */
public class QuoteParseBolt implements IRichBolt{
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

    }

    public void execute(Tuple tuple) {

    }

    public void cleanup() {

    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
