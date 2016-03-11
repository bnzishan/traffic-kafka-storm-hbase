package com.nano.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Map;

/**
 * Created by Administrator on 2016/3/8.
 */
public class HBaseBolt extends BaseRichBolt {
    private OutputCollector outputCollector;
    private Connection connection;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.outputCollector = collector;
        Configuration cfg = HBaseConfiguration.create();
        try {
            connection = ConnectionFactory.createConnection(cfg);
        } catch (IOException e) {
            e.printStackTrace();
        }
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

        try {
            Admin admin = connection.getAdmin();
            TableName tableName = TableName.valueOf("traffic");
            if (admin.tableExists(tableName)) {


                Put put = new Put(Bytes.toBytes(row));
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("area"), Bytes.toBytes(area));
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("year"), Bytes.toBytes(year));
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("month"), Bytes.toBytes(month));
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("date"), Bytes.toBytes(date));
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("hour"), Bytes.toBytes(hour));
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("minute"), Bytes.toBytes(minute));
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("count"), Bytes.toBytes(count));
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("speed"), Bytes.toBytes(speed));

                admin.getConnection().getTable(tableName).put(put);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        outputCollector.ack(input);
        System.out.println("put count : " + count);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
