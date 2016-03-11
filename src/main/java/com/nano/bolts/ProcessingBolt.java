package com.nano.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.storm.shade.com.google.common.util.concurrent.AtomicDouble;

import java.io.IOException;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by Administrator on 2016/3/8.
 */
public class ProcessingBolt extends BaseRichBolt {
    private OutputCollector outputCollector;
    private Connection connection;
    private Map<String, AtomicLong> countMap;
    private Map<String, AtomicDouble> speedMap;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.outputCollector = collector;
        countMap = new HashMap<>();
        speedMap = new HashMap<>();
        Configuration cfg = HBaseConfiguration.create();
        try {
            connection = ConnectionFactory.createConnection(cfg);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void execute(Tuple input) {
        String area = input.getString(0);
        double speed = input.getDouble(1);
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

        outputCollector.emit(input, new Values(row, area, year, month, date, hour, minute, count.get(), speedAvg.get()));
        outputCollector.ack(input);
        System.out.println(countMap);


    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("row", "area", "year", "month", "date", "hour", "minute", "count", "speed"));
    }
}
