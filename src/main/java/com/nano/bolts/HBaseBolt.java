package com.nano.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.nano.Application;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
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
        String area = input.getString(0);
        Double speed = input.getDouble(1);
        try {
            Admin admin = connection.getAdmin();
            TableName tableName = TableName.valueOf("traffic");
            if (admin.tableExists(tableName)) {
                Get get = new Get(Bytes.toBytes(area));
                Result result = admin.getConnection().getTable(tableName).get(get);

                Long count = 0L;
                Double speedVal = 0.0D;
                if (!result.isEmpty()) {
                    count = Bytes.toLong(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("count")));
                    speedVal = Bytes.toDouble(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("speed")));
                }
                Double speedSum = count * speedVal + speed;
                Double speedAvg = speedSum / ++count;
                List<Put> puts = new ArrayList<>();
                Put putArea = new Put(Bytes.toBytes(area));
                putArea.addColumn(Bytes.toBytes("info"),Bytes.toBytes("area"),Bytes.toBytes(area));
                Put putCount = new Put(Bytes.toBytes(area));
                putCount.addColumn(Bytes.toBytes("info"),Bytes.toBytes("count"),Bytes.toBytes(count));
                Put putSpeed = new Put(Bytes.toBytes(area));
                putSpeed.addColumn(Bytes.toBytes("info"),Bytes.toBytes("speed"),Bytes.toBytes(speedAvg));
                puts.add(putArea);
                puts.add(putSpeed);
                puts.add(putCount);
                admin.getConnection().getTable(tableName).put(puts);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
