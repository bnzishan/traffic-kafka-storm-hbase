package com.nano.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.nano.utils.BloomFilter;
import org.codehaus.jackson.map.ObjectMapper;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;

/**
 * Created by Administrator on 2016/3/11.
 */
public class MessageSplitBolt extends BaseRichBolt {
    private OutputCollector outputCollector;
    private Jedis jedis;//非切片额客户端连接
    private JedisPool jedisPool;//非切片连接池
    private BloomFilter bloomFilter;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.outputCollector = collector;

        // 池基本配置
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(20);
        config.setMaxIdle(5);
        config.setMaxWaitMillis(1000l);
        config.setTestOnBorrow(false);

        jedisPool = new JedisPool(config, "redis", 6379);

        jedis = jedisPool.getResource();

        bloomFilter = new BloomFilter();
    }

    @Override
    public void execute(Tuple input) {
        ObjectMapper objectMapper = new ObjectMapper();
        String msg = input.getString(0);
        try {
            Map<String, Object> info = objectMapper.readValue(msg, Map.class);
            String area = info.get("area") == null ? "default" : info.get("area").toString();
            double speed = 0.00d;
            try{
                speed = (double) info.get("speed");
            }catch(Exception e) {
                e.printStackTrace();
            }
            String uuid = info.get("uuid") == null ? UUID.randomUUID().toString() : info.get("uuid").toString();
            String key = "splitter-" + uuid;
            if (!bloomFilter.contains(key)) {
                bloomFilter.add(key);
                outputCollector.emit(input, new Values(area, speed, uuid));
            }
//            if(!jedis.exists(key)){
//                jedis.incr(key);
//                outputCollector.emit(input, new Values(area, speed,uuid));
//            }
            outputCollector.ack(input);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("area", "speed", "uuid"));
    }
}
