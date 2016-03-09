package com.nano;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import com.nano.bolts.HBaseBolt;
import com.nano.bolts.ProcessingBolt;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import storm.kafka.*;

import java.io.IOException;
import java.util.Arrays;

/**
 * Created by Administrator on 2016/3/8.
 */
public class Application {

    public static void main(String... args){
        System.setProperty("hadoop.home.dir", "D:/hadoop");
        String zks = "zookeeper1:2181,zookeeper2:2181,zookeeper3:2181";
        String topic = "traffic-info-topic";
        String zkRoot = "/storm"; // default zookeeper root configuration for storm
        String id = "word";

        BrokerHosts brokerHosts = new ZkHosts(zks);
        SpoutConfig spoutConf = new SpoutConfig(brokerHosts, topic, zkRoot, id);
        spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
        spoutConf.zkServers = Arrays.asList(new String[] {"zookeeper1", "zookeeper2", "zookeeper3"});
        spoutConf.zkPort = 2181;


        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("reader",new KafkaSpout(spoutConf), 5);
        builder.setBolt("process",new ProcessingBolt(),5).shuffleGrouping("reader");
        builder.setBolt("writer",new HBaseBolt()).shuffleGrouping("process");

        Config conf = new Config();
        conf.setMaxTaskParallelism(3);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(Application.class.getSimpleName(), conf, builder.createTopology());
    }
}
