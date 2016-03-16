package com.nano.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.nano.utils.BloomFilter;
import org.apache.storm.shade.com.google.common.util.concurrent.AtomicDouble;

import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by Administrator on 2016/3/8.
 */
public class ProcessingBolt extends BaseRichBolt {
    private OutputCollector outputCollector;
    private Map<String, AtomicLong> countMap;
    private Map<String, AtomicDouble> speedMap;
//    private Jedis jedis;//非切片额客户端连接
//    private JedisPool jedisPool;//非切片连接池
    private BloomFilter bloomFilter;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.outputCollector = collector;
        countMap = new HashMap<>();
        speedMap = new HashMap<>();

        // 池基本配置
//        JedisPoolConfig config = new JedisPoolConfig();
//        config.setMaxTotal(20);
//        config.setMaxIdle(5);
//        config.setMaxWaitMillis(1000l);
//        config.setTestOnBorrow(false);
//
//        jedisPool = new JedisPool(config, "redis", 6379);
//
//        jedis = jedisPool.getResource();

        bloomFilter = new BloomFilter();
    }

    @Override
    public void execute(Tuple input) {
        String area = input.getString(0);
        double speed = input.getDouble(1);
        String uuid = input.getString(2);

        String key = "processor-" + uuid;

        if(!bloomFilter.contains(key)) {
            bloomFilter.add(key);
            Calendar calendar = Calendar.getInstance();
            long year = calendar.get(Calendar.YEAR);
            long month = calendar.get(Calendar.MONTH) + 1;
            long date = calendar.get(Calendar.DATE);
            long hour = calendar.get(Calendar.HOUR_OF_DAY);
            long minute = calendar.get(Calendar.MINUTE) / 10 + 1;

            String row = area + year + month + date + hour + minute;
            AtomicLong count = this.countMap.get(row);
            if (count == null) {
                count = new AtomicLong();
                this.countMap.put(row, count);
            }

            AtomicDouble speedAvg = this.speedMap.get(row);
            if (speedAvg == null) {
                speedAvg = new AtomicDouble();
                this.speedMap.put(row, speedAvg);
            }
            speedAvg.set((count.get() * speedAvg.get() + speed) / (count.get() + 1));
            count.addAndGet(1);
            outputCollector.emit(input, new Values(row, area, year, month, date, hour, minute, count.get(), speedAvg.get(), uuid));
        }
//        if (!jedis.exists(key)) {
//            jedis.incr(key);
//
//            Calendar calendar = Calendar.getInstance();
//            long year = calendar.get(Calendar.YEAR);
//            long month = calendar.get(Calendar.MONTH) + 1;
//            long date = calendar.get(Calendar.DATE);
//            long hour = calendar.get(Calendar.HOUR_OF_DAY);
//            long minute = calendar.get(Calendar.MINUTE) / 10 + 1;
//
//            String row = area + year + month + date + hour + minute;
//            AtomicLong count = this.countMap.get(row);
//            if (count == null) {
//                count = new AtomicLong();
//                this.countMap.put(row, count);
//            }
//
//            AtomicDouble speedAvg = this.speedMap.get(row);
//            if (speedAvg == null) {
//                speedAvg = new AtomicDouble();
//                this.speedMap.put(row, speedAvg);
//            }
//            speedAvg.set((count.get() * speedAvg.get() + speed) / (count.get() + 1));
//            count.addAndGet(1);
//            outputCollector.emit(input, new Values(row, area, year, month, date, hour, minute, count.get(), speedAvg.get(), uuid));
//        }
        outputCollector.ack(input);
//        System.out.println(countMap);


    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("row", "area", "year", "month", "date", "hour", "minute", "count", "speed", "uuid"));
    }
}
