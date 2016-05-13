package com.nano;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.transactional.TransactionalTopologyBuilder;
import backtype.storm.tuple.Fields;
import com.nano.bolts.HBaseBolt;
import com.nano.bolts.MessageSplitBolt;
import com.nano.bolts.ProcessingBolt;
import storm.kafka.*;
import storm.trident.TridentState;
import storm.trident.TridentTopology;

import java.util.Arrays;

/**
 * Created by Administrator on 2016/3/8.
 */
public class Application {

    public static void main(String... args) throws Exception {
        String zks = "zookeeper04:2181,zookeeper05:2181,zookeeper06:2181";
        String topic = "traffic-info-topic";
        String zkRoot = "/jstorm"; // default zookeeper root configuration for storm
        String id = "word";

        BrokerHosts brokerHosts = new ZkHosts(zks);
        SpoutConfig spoutConf = new SpoutConfig(brokerHosts, topic, zkRoot, id);
        spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
        spoutConf.zkServers = Arrays.asList(new String[]{"zookeeper04", "zookeeper05", "zookeeper06"});
        spoutConf.zkPort = 2181;

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("reader", new KafkaSpout(spoutConf), 8).setNumTasks(20);
        builder.setBolt("splitter", new MessageSplitBolt(), 4).shuffleGrouping("reader").setNumTasks(10);
        builder.setBolt("processor", new ProcessingBolt(), 4).fieldsGrouping("splitter", new Fields("area")).setNumTasks(10); // 相同区域交由同一个bolt处理
        builder.setBolt("writer",new HBaseBolt(),12).fieldsGrouping("processor",new Fields("row")).setNumTasks(20); // 相同key交由同一个bolt处理
//        builder.setBolt("writer", new RedisBolt(), 8).fieldsGrouping("processor", new Fields("row")).setNumTasks(20); // 相同key交由同一个bolt处理
//        builder.setBolt("writer", new MySqlBolt(), 8).fieldsGrouping("processor", new Fields("row")).setNumTasks(20); // 相同key交由同一个bolt处理

        Config conf = new Config();
        String name = Application.class.getSimpleName();
        if (args != null && args.length > 0) {
            // Nimbus host name passed from command line
            conf.put(Config.TOPOLOGY_TRANSFER_BUFFER_SIZE, 32);
            conf.put(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE, 16384);
            conf.put(Config.TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE, 16384);
            conf.put(Config.NIMBUS_HOST, args[0]);
            conf.setNumWorkers(5);
            StormSubmitter.submitTopologyWithProgressBar(name, conf, builder.createTopology());
        } else {
            System.setProperty("hadoop.home.dir", "D:/hadoop");
            conf.setMaxTaskParallelism(3);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(name, conf, builder.createTopology());
        }
    }
}
