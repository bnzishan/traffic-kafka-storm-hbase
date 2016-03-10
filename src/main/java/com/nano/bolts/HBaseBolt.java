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
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

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
        Calendar calendar = Calendar.getInstance();
        int year = calendar.get(Calendar.YEAR);
        int month = calendar.get(Calendar.MONTH) + 1;
        int date = calendar.get(Calendar.DATE);
        int hour = calendar.get(Calendar.HOUR_OF_DAY);
        int minute = calendar.get(Calendar.MINUTE) / 10 + 1;

        try {
            Admin admin = connection.getAdmin();
            TableName tableName = TableName.valueOf("traffic");
            if (admin.tableExists(tableName)) {
                Long count = 0L;
                Double speedVal = 0.0D;
                FilterList filterList = new FilterList();
                Filter areaFilter = new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("area"), CompareFilter.CompareOp.EQUAL, Bytes.toBytes(area));
                Filter yearFilter = new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("year"), CompareFilter.CompareOp.EQUAL, Bytes.toBytes(year));
                Filter monthFilter = new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("month"), CompareFilter.CompareOp.EQUAL, Bytes.toBytes(month));
                Filter dateFilter = new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("date"), CompareFilter.CompareOp.EQUAL, Bytes.toBytes(date));
                Filter hourFilter = new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("hour"), CompareFilter.CompareOp.EQUAL, Bytes.toBytes(hour));
                Filter minuteFilter = new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("minute"), CompareFilter.CompareOp.EQUAL, Bytes.toBytes(minute));
                filterList.addFilter(areaFilter);
                filterList.addFilter(yearFilter);
                filterList.addFilter(monthFilter);
                filterList.addFilter(dateFilter);
                filterList.addFilter(hourFilter);
                filterList.addFilter(minuteFilter);

                Scan scan = new Scan();
                scan.setFilter(filterList);
                ResultScanner rs = admin.getConnection().getTable(tableName).getScanner(scan);
                Result result = rs.next();

//                Get get = new Get(Bytes.toBytes(point));
//                get.setFilter(filterList);
//                Result result = admin.getConnection().getTable(tableName).get(get);

                String row = area + System.currentTimeMillis();
                if (result != null) {
                    row = Bytes.toString(result.getRow());
                    count = Bytes.toLong(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("count")));
                    speedVal = Bytes.toDouble(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("speed")));
                }

                Double speedSum = count * speedVal + speed;
                Double speedAvg = speedSum / ++count;

                Put put = new Put(Bytes.toBytes(row));
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("area"), Bytes.toBytes(area));
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("count"), Bytes.toBytes(count));
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("year"), Bytes.toBytes(year));
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("month"), Bytes.toBytes(month));
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("date"), Bytes.toBytes(date));
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("hour"), Bytes.toBytes(hour));
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("minute"), Bytes.toBytes(minute));
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("speed"), Bytes.toBytes(speedAvg));

                admin.getConnection().getTable(tableName).put(put);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        outputCollector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
