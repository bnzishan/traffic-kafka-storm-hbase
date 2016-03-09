package com.nano.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.Map;

/**
 * Created by Administrator on 2016/3/8.
 */
public class ProcessingBolt extends BaseRichBolt {
    private OutputCollector outputCollector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.outputCollector = collector;
    }

    @Override
    public void execute(Tuple input) {
        ObjectMapper objectMapper = new ObjectMapper();
        String message = input.getString(0);
        Map<String, Object> info = null;
        try {
            info = objectMapper.readValue(message, Map.class);
        } catch (IOException e) {
            e.printStackTrace();
        }

        outputCollector.emit(input, new Values(info.get("area"), info.get("speed")));
        outputCollector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("area", "speed"));
    }
}
