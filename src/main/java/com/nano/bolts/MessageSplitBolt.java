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
 * Created by Administrator on 2016/3/11.
 */
public class MessageSplitBolt extends BaseRichBolt {
    private OutputCollector outputCollector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.outputCollector = collector;
    }

    @Override
    public void execute(Tuple input) {
        ObjectMapper objectMapper = new ObjectMapper();
        String msg = input.getString(0);
        try {
            Map<String, Object> info = objectMapper.readValue(msg, Map.class);
            String area = info.get("area").toString();
            double speed = (double) info.get("speed");
            outputCollector.emit(input, new Values(area, speed));
            outputCollector.ack(input);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("area", "speed"));
    }
}
