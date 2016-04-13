package com.nano.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.google.common.collect.Maps;
import com.nano.utils.BloomFilter;
import org.apache.storm.jdbc.common.ConnectionProvider;
import org.apache.storm.jdbc.common.HikariCPConnectionProvider;
import org.apache.storm.jdbc.common.JdbcClient;

import java.util.Map;

/**
 * Created by Administrator on 2016/3/14.
 */
public class MySqlBolt extends BaseRichBolt {
    private OutputCollector outputCollector;
    private BloomFilter bloomFilter;
    private Map<String, Boolean> rowExist;
    private JdbcClient client;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.outputCollector = collector;
        Map hikariConfigMap = Maps.newHashMap();
        hikariConfigMap.put("dataSourceClassName", "com.mysql.jdbc.jdbc2.optional.MysqlDataSource");
        hikariConfigMap.put("dataSource.url", "jdbc:mysql://mysql01:3306/traffic?useUnicode=true&characterEncoding=utf-8");
        hikariConfigMap.put("dataSource.user", "nano");
        hikariConfigMap.put("dataSource.password", "123456");
        ConnectionProvider connectionProvider = new HikariCPConnectionProvider(hikariConfigMap);
        connectionProvider.prepare();
        int queryTimeoutSecs = 60;
        this.client = new JdbcClient(connectionProvider, queryTimeoutSecs);
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
            String sql;
            if (!rowExist.containsKey(row)) {
                sql = String.format("insert into real_time_traffic_info (id,area, speed, count, year, month, date, hour, minute,created,modified) values ('%s','%s',%f,%d,%d,%d,%d,%d,%d,now(),now())", row, area, speed, count, year, month, date, hour, minute);
                rowExist.put(row, Boolean.TRUE);
            } else {
                sql = String.format("update real_time_traffic_info set count = %d, speed = %f, modified = now() where id = '%s'", count, speed, row);
            }
            client.executeSql(sql);
        }

        outputCollector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
