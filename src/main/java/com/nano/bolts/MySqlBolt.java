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
//        PoolProperties p = new PoolProperties();
//        p.setUrl("jdbc:mysql://mysql01:3306/traffic?useUnicode=true&characterEncoding=utf-8");
//        p.setDriverClassName("com.mysql.jdbc.Driver");
//        p.setUsername("nano");
//        p.setPassword("123456");
//        p.setJmxEnabled(true);
//        p.setTestWhileIdle(false);
//        p.setTestOnBorrow(true);
//        p.setValidationQuery("SELECT 1");
//        p.setTestOnReturn(false);
//        p.setValidationInterval(30000);
//        p.setTimeBetweenEvictionRunsMillis(30000);
//        p.setMaxActive(2000);
//        p.setInitialSize(1000);
//        p.setMaxWait(10000);
//        p.setRemoveAbandonedTimeout(6000);
//        p.setMinEvictableIdleTimeMillis(30000);
//        p.setMinIdle(10);
//        p.setLogAbandoned(true);
//        p.setRemoveAbandoned(true);
//        p.setJdbcInterceptors("org.apache.tomcat.jdbc.pool.interceptor.ConnectionState;" +
//                "org.apache.tomcat.jdbc.pool.interceptor.StatementFinalizer");
//        datasource = new DataSource();
//        datasource.setPoolProperties(p);
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
            if (!rowExist.containsKey(row)) {
                String sql = String.format("insert into real_time_traffic_info (id,area, speed, count, year, month, date, hour, minute,created,modified) values ('%s','%s',%f,%d,%d,%d,%d,%d,%d,now(),now())",row,area,speed,count,year,month,date,hour,minute);
                client.executeSql(sql);
//                List<Column> columns = Lists.newArrayList(
//                        new Column("id", row, Types.INTEGER),
//                        new Column("area", area, Types.VARCHAR),
//                        new Column("count", count, Types.BIGINT),
//                        new Column("speed", speed, Types.DOUBLE),
//                        new Column("year", year, Types.BIGINT),
//                        new Column("month", month, Types.BIGINT),
//                        new Column("date", date, Types.BIGINT),
//                        new Column("hour", hour, Types.BIGINT),
//                        new Column("minute", minute, Types.BIGINT),
//                        new Column("created", new Timestamp(System.currentTimeMillis()), Types.TIMESTAMP),
//                        new Column("modified", new Timestamp(System.currentTimeMillis()), Types.TIMESTAMP));
//                List<List<Column>> moreRows = new ArrayList<>();
//                moreRows.add(columns);
//                client.insert("real_time_traffic_info", Lists.newArrayList(moreRows));
//                    preparedStatement = connection.prepareStatement("insert into real_time_traffic_info (id,area, speed, count, year, month, date, hour, minute,created,modified) values (?,?,?,?,?,?,?,?,?,now(),now())");
//                    preparedStatement.setString(1, row);
//                    preparedStatement.setString(2, area);
//                    preparedStatement.setDouble(3, speed);
//                    preparedStatement.setLong(4,count);
//                    preparedStatement.setLong(5,year);
//                    preparedStatement.setLong(6,month);
//                    preparedStatement.setLong(7,date);
//                    preparedStatement.setLong(8,hour);
//                    preparedStatement.setLong(9,minute);
//                    preparedStatement.executeUpdate();
                rowExist.put(row, Boolean.TRUE);
            } else {
                String sql = String.format("update real_time_traffic_info set count = %d, speed = %f, modified = now() where id = '%s'",count,speed,row);
                client.executeSql(sql);
//                    preparedStatement = connection.prepareStatement("update real_time_traffic_info set count = ?, speed = ?, modified = now() where id = ?");
//                    preparedStatement.setLong(1, count);
//                    preparedStatement.setDouble(2, speed);
//                    preparedStatement.setString(3, row);
//                    preparedStatement.executeUpdate();
            }
//                preparedStatement.close();
//                connection.close();

        }

        outputCollector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
