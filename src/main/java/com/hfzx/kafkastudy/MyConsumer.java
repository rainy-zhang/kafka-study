package com.hfzx.kafkastudy;

import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.Properties;

/**
 * @Author: zhangyu
 * @Description: 消息消费者
 * @Date: in 2020/5/21 20:26
 */
@SuppressWarnings("all")
public class MyConsumer {

    private static KafkaConsumer<String, String> consumer;
    private static Properties properties;

    static {
        properties = new Properties();
        properties.put("bootstrap.servers", "127.0.0.1:9092");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        // 指定消费者组, 每个消费者组都可以消费kafka中的全量信息
        properties.put("group.id", "KafkaStudy");
    }


    /**
     * 消费者对象不是线程安全的,一个线程对应一个消费者
     */

    // 自动提交消息位移
    private static void generalConsumerMessageAutoCommit() {
        // 设置自动提交位移
        properties.put("enable.auto.commit", true);
        consumer = new KafkaConsumer<>(properties);
        // 订阅topic
        consumer.subscribe(Collections.singleton("kafka-study"));

        // 循环拉取kafka中的消息
        try {
            while (true) {
                boolean flag = true;
                // 调用poll拉取数据
                ConsumerRecords<String, String> records = consumer.poll(100);

                for (ConsumerRecord<String, String> record : records) {
                    System.out.println(String.format("topic = %s, partition = %s, key = %s, value = %s", record.topic(), record.partition(), record.key(), record.value()));
                    if (record.value().equals("done")) {
                        flag = false;
                    }
                }
                if (!flag) {
                    break;
                }
            }
        } finally {
            consumer.close();
        }
    }

    //手动同步提交位移, 提交poll返回的最后位移
    private static void generalConsumerMessageSyncCommit() {
        // 设置手动提交位移
        properties.put("auto.commit.offset", false);
        consumer = new KafkaConsumer<>(properties);
        // 订阅topic
        consumer.subscribe(Collections.singleton("kafka-stidy"));

        while(true) {
            boolean flag = true;
            // 调用poll方法拉取数据
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(String.format("topic = %s, partition = %s, key = %s, value = %s", record.topic(), record.partition(), record.key(), record.value()));
                if (record.value().equals("done")) {
                    flag = false;
                }
            }
            // 使用consumer同步提交位移, 调用commitSync()会导致当前线程阻塞,
            // 如果服务器返回提交失败,同步提交会进行重试,直到提交成功或抛出CommitFailedException异常
            try {
                consumer.commitSync();
            } catch (CommitFailedException e) {
                e.printStackTrace();
            }

            if (!flag)
                break;
        }
    }

    // 手动异步提交位移
    private static void generalConsumerMessageAsyncCommit() {
        // 设置手动提交位移
        properties.put("auto.commit.offset", false);
        consumer = new KafkaConsumer<>(properties);
        // 订阅topic
        consumer.subscribe(Collections.singleton("kafka-stidy"));

        while(true) {
            boolean flag = true;
            // 调用poll方法拉取数据
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(String.format("topic = %s, partition = %s, key = %s, value = %s", record.topic(), record.partition(), record.key(), record.value()));
                if (record.value().equals("done")) {
                    flag = false;
                }
            }
            // 使用consumer异步提交位移, 如果服务器返回提交失败,异步提交不会进行重试,同步提交会进行重试,直到提交成功或抛出CommitFailedException异常
            // commit A, offset 2000
            // commit B, offset 3000
            // A提交失败, B提交成功, A重新提交后成功, 那么会导致最后一次提交的offset为2000, 所有消费者都将从offset: 2000这个位置重新开始消费消息, 会导致2000-3000这段数据被重复消费
            consumer.commitAsync();

            if (!flag)
                break;
        }
    }

    // 手动异步提交消息位移带回调函数 会记录下失败的情况
    private static void generalConsumerMessageAsyncCommitCallback() {
        // 设置手动提交位移
        properties.put("auto.commit.offset", false);
        consumer = new KafkaConsumer<>(properties);
        // 订阅topic
        consumer.subscribe(Collections.singleton("kafka-stidy"));

        while(true) {
            boolean flag = true;
            // 调用poll方法拉取数据
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(String.format("topic = %s, partition = %s, key = %s, value = %s", record.topic(), record.partition(), record.key(), record.value()));
                if (record.value().equals("done")) {
                    flag = false;
                }
            }
            // key: topic,partition value: offset,元数据
            consumer.commitAsync((map, e) -> {
                // 如果想进行重试同时又保证消息顺序的话, 可以使用单调递增的序号,
                // 就是说每次发起异步提交的时候增加一个序号,并且将这个序号作为参数传递给回调方法,当消息提交失败回调时,检查参数中的序号值与全局的序号值是否相等,如果相等就说明没有新的消息,就可以进行重试,否则就放弃提交,因为已经有更新的位移提交了
                // 对于大部分情况下,在使用kafka消费消息与生产消息,位移提交失败是非常少见的
                if (e != null) {
                    System.out.println("commit failed for offset: " + e.getMessage());
                }
            });
            if (!flag)
                break;
        }
    }

    // 混合同步提交与异步提交,
    // 在正常情况下偶然的提交失败并不是什么大问题,因为后续的提交成功会把提交失败的位移覆盖掉,但是在某些情况下,
    // 比如程序退出了,我们希望最后的提交也能够成功就可以使用混合同步提交与异步提交
    private static void mixSyncAndAsyncCommit() {
        // 设置手动提交位移
        properties.put("auto.commit.offset", false);
        consumer = new KafkaConsumer<>(properties);
        // 订阅topic
        consumer.subscribe(Collections.singleton("kafka-stidy"));

        try {
            while(true) {
                boolean flag = true;
                // 调用poll方法拉取数据
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println(String.format("topic = %s, partition = %s, key = %s, value = %s", record.topic(), record.partition(), record.key(), record.value()));
                    if (record.value().equals("done")) {
                        flag = false;
                    }
                }
                // 混合同步提交与异步提交
                //先进行异步提交
                consumer.commitAsync();

                if (!flag)
                    break;
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            // 如果异步提交失败了, 则使用同步提交来尽最大可能保证提交成功, 因为同步提交本身就有重试的过程
            try {
                consumer.commitSync();
            } finally {
                consumer.close();
            }
        }
    }

    public static void main(String[] args) {
        generalConsumerMessageAutoCommit();
    }

}
