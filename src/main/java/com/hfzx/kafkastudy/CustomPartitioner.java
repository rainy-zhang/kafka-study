package com.hfzx.kafkastudy;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.record.InvalidRecordException;
import org.apache.kafka.common.utils.Utils;

import java.util.*;

/**
 * @Author: zhangyu
 * @Description: 自定义的分区分配器
 * @Date: in 2020/5/21 19:54
 */
public class CustomPartitioner implements Partitioner {

    /**
     * 自定义分区
     * @param topic topic
     * @param key 消息的key
     * @param keyBytes key的字节表示
     * @param value 消息的value
     * @param valueBytes value的字节表示
     * @param cluster kafka集群的信息
     * @return 分区号
     */
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        // 获取topic中所有的partition
        List<PartitionInfo> partitionInfos = cluster.partitionsForTopic(topic);
        int numPartitions = partitionInfos.size();

        // 根据key来进行分区, 如果key为空则抛出异常
        if (null != keyBytes && !(key instanceof String)) {
            throw new InvalidRecordException("kafka message must have key");
        }

        if (numPartitions == 1) {
            return 0;
        }

        if (key.equals("name")) {
            return numPartitions - 1;
        }

        //使用kafka默认的分配策略: 根据key获取hash值对分区数进行取余来确定发送到哪个分区
        return Math.abs(Utils.murmur2(keyBytes)) % (numPartitions - 1);
    }

    /**
     * 关闭分配器
     */
    @Override
    public void close() {

    }

    /**
     * 对分区分配器进行配置
     * @param map
     */
    @Override
    public void configure(Map<String, ?> map) {

    }

}
