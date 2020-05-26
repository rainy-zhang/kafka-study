package com.hfzx.kafkastudy;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

/**
 * @Author: zhangyu
 * @Description: 发送消息
 * @Date: in 2020/5/19 20:52
 */
public class MyProducer {

    private static KafkaProducer<String, String> producer;

    static {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "127.0.0.1:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //配置自定义分区分配器
        properties.put("partitioner.class", "com.hfzx.kafkastudy.CustomPartitioner");

        producer = new KafkaProducer<>(properties);
    }

    /**
     * Message中key的作用:
     * kafka会根据message中的key来决定消息写入哪个分区(partition), 所有具有相同key的消息会被写入到同一个partition中
     * 不存在key, 即key为null时,kafka会使用默认的分区分配器,round-robin实现负载均衡
     * 存在key时,kafka会使用默认的分区分配器,对key进行hash确定消息应该分配到哪个分区
     * 默认的分区分配器: default partitioner会使用轮训的的算法,来将消息写入到分区中来实现负载均衡
     * 计算消息与分区的映射关系,计算的是全部的分区,而不是可用的分区,所以当消息被分配到不可用的分区时会写入失败,
     * 如果需要加入新的分区那么消息与分区的映射也会发生变化,应避免这种情况发生
     */

    // 向Kafka推送消息，不关心结果
    private static void sendMessageForgetResult() {
        ProducerRecord<String, String> record = new ProducerRecord<>("kafka-study", "name", "ForgetResult");
        producer.send(record);
        producer.close();
    }

    // 向Kafka同步发送数据
    private static void sendMessageSync() throws Exception {
        ProducerRecord<String, String> record = new ProducerRecord<>("kafka-study", "name", "sync");
        // 调用get()方法, 会等待kafka集群的响应. 当发送失败时会抛出异常.
        // 1. 不可恢复的异常(如:发送的消息过大), 2. 可恢复的异常broker会进行重试,一定次数之后才会抛出异常(如:连接超时)
        RecordMetadata result = producer.send(record).get();
        System.out.println(result.topic());
        System.out.println(result.partition());
        System.out.println(result.offset());
    }

    // 向Kafka异步发送数据
    private static void sendMessageCallback() {
        ProducerRecord<String, String> record = new ProducerRecord<>("kafka-study", "name", "callback");
        producer.send(record, new MyProducerCallback());
        record = new ProducerRecord<>("kafka-study", "name", "callback");
        producer.send(record, new MyProducerCallback());
        record = new ProducerRecord<>("kafka-study", "name", "callback");
        producer.send(record, new MyProducerCallback());
        record = new ProducerRecord<>("kafka-study", "name", "done");
        producer.send(record, new MyProducerCallback());
        producer.close();
    }

    // 自定义回调类, 需要实现org.apache.kafka.clients.producer.Callback接口
    private static class MyProducerCallback implements Callback {
        @Override
        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            if (null != e) {
                e.printStackTrace();
                return;
            }
            System.out.println(recordMetadata.topic());
            System.out.println(recordMetadata.partition());
            System.out.println(recordMetadata.offset());
            System.out.println("Coming in MyProducerCallback");
        }
    }

    public static void main(String[] args) throws Exception {
//        sendMessageForgetResult();
//        sendMessageSync();
        sendMessageCallback();
    }
}
