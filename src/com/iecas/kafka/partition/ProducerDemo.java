package com.iecas.kafka.partition;


import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;

/**
 * 使用自定义分区规则，向kafka—topic的不同分区发送数据
 * Created by Administrator on 2016/1/5.
 */
public class ProducerDemo {

    public static void main(String args[]) {
        ProducerDemo.Service();
    }

    public static void Service() {
        try {
            final Thread thread = new Thread(new Runnable() {
                @Override
                public void run() {
                    Properties props = new Properties();
                    props.put("serializer.class", "kafka.serializer.StringEncoder");
                    props.put("metadata.broker.list", "192.168.5.251:9092,192.168.5.252:9092,192.168.5.253:9092");
                    props.put("zk.connect", "192.168.5.251:2181,192.168.5.252:2181,192.168.5.253:2181/kafka");
                    //配置自定义分区的规则类
                    props.put("partitioner.class", "com.iecas.kafka.partition.MyPartitioner");
                    Producer<String, String> producer = new Producer<String, String>(new ProducerConfig(props));
                    //配置topic
                    String topic = "part1";
                    for (int i = 0; i < 100; i++) {
                        String k = i + "";
                        String v = k + "--value";
                        producer.send(new KeyedMessage<String, String>(topic, k, v));
                    }
                    producer.close();
                }
            });
            thread.start();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
