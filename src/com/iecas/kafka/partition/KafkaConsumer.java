package com.iecas.kafka.partition;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 使用Kafka原生API，用于测试多个线程访问topic里面多个分区的数据
 * <p>
 * Created by hacker on 2016/1/19.
 */
public class KafkaConsumer implements Runnable {

    private ConsumerConfig config;              //消费者配置
    private static String topic = "part1";       //消费的topic名
    Properties props;                           //配置文件对象
    final int a_numThreads = 5;                 //topic的分区数

    /**
     * 初始化消费者配置信息
     */
    public KafkaConsumer() {
        props = new Properties();
        props.put("zookeeper.connect", "192.168.5.251:2181,192.168.5.252:2181,192.168.5.253:2181/kafka");
        props.put("group.id", "group1");
        props.put("auto.commit.interval.ms", "1000");
        props.put("auto.offset.reset", "smallest");
        props.put("zookeeper.session.timeout.ms", "400");
        config = new ConsumerConfig(props);
    }

    @Override
    public void run() {
        Map<String, Integer> topicCountMap = new HashMap<>();
        topicCountMap.put(topic, new Integer(a_numThreads));
        ConsumerConfig config = new ConsumerConfig(props);
        ConsumerConnector connector = kafka.consumer.Consumer.createJavaConsumerConnector(config);
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = connector.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);
        ExecutorService executorService = Executors.newFixedThreadPool(a_numThreads);
        for (final KafkaStream stream : streams) {
            executorService.submit(new KafkaConsumerThread(stream));
        }
    }

    public static void main(String[] args) {
        Thread t = new Thread(new KafkaConsumer());
        t.start();
    }
}


class KafkaConsumerThread implements Runnable {

    private KafkaStream<byte[], byte[]> stream;

    public KafkaConsumerThread(KafkaStream<byte[], byte[]> stream) {
        this.stream = stream;
    }

    @Override
    public void run() {
        ConsumerIterator<byte[], byte[]> it = stream.iterator();
        while (it.hasNext()) {
            MessageAndMetadata<byte[], byte[]> mam = it.next();
            System.out.println(Thread.currentThread().getName() + ":partition[" +
                    mam.partition() + "]" + "Offset[" + mam.offset() + "]" + new String(mam.message()));
        }
    }
}