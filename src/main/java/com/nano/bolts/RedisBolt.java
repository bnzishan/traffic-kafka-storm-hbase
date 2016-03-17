package com.nano.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.google.common.collect.Maps;
import com.nano.utils.BloomFilter;
import org.apache.hadoop.hbase.util.Bytes;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.Date;
import java.util.Map;

/**
 * Created by Administrator on 2016/3/14.
 */
public class RedisBolt extends BaseRichBolt {
    private OutputCollector outputCollector;
    private BloomFilter bloomFilter;
    private Map<String, Boolean> rowExist;
    private JedisPool jedisPool;//非切片连接池

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.outputCollector = collector;

//        池基本配置
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(500);
        config.setMaxIdle(50);
        config.setMaxWaitMillis(1000l);
        config.setTestOnBorrow(false);
        jedisPool = new JedisPool(config, "redis", 6379);
        bloomFilter = new BloomFilter();
        rowExist = Maps.newHashMap();
    }

    @Override
    public void execute(Tuple input) {
        String row = input.getString(0);
        String area = input.getString(1);
        long year = input.getLong(2);
        long month = input.getLong(3);
        long date = input.getLong(4);
        long hour = input.getLong(5);
        long minute = input.getLong(6);
        long count = input.getLong(7);
        double speed = input.getDouble(8);
        String uuid = input.getString(9);

        String key = "hbase-" + uuid;

        if (!bloomFilter.contains(key)) {
            bloomFilter.add(key);
            Jedis jedis = jedisPool.getResource();
            jedis.hset(row.getBytes(), Bytes.toBytes("area"), Bytes.toBytes(area));
            jedis.hset(row.getBytes(), Bytes.toBytes("count"), Bytes.toBytes(count));
            jedis.hset(row.getBytes(), Bytes.toBytes("speed"), Bytes.toBytes(speed));
            jedis.hset(row.getBytes(), Bytes.toBytes("year"), Bytes.toBytes(year));
            jedis.hset(row.getBytes(), Bytes.toBytes("month"), Bytes.toBytes(month));
            jedis.hset(row.getBytes(), Bytes.toBytes("date"), Bytes.toBytes(date));
            jedis.hset(row.getBytes(), Bytes.toBytes("hour"), Bytes.toBytes(hour));
            jedis.hset(row.getBytes(), Bytes.toBytes("minute"), Bytes.toBytes(minute));
            byte[] created = jedis.hget(row.getBytes(), Bytes.toBytes("created"));
            if (created == null) {
                jedis.hset(row.getBytes(), Bytes.toBytes("created"), Bytes.toBytes(new Date().getTime()));
                jedis.hset(row.getBytes(), Bytes.toBytes("modified"), Bytes.toBytes(new Date().getTime()));
            } else {
                jedis.hset(row.getBytes(), Bytes.toBytes("created"), created);
                jedis.hset(row.getBytes(), Bytes.toBytes("modified"), Bytes.toBytes(new Date().getTime()));
            }
            jedis.close();
        }

        outputCollector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
