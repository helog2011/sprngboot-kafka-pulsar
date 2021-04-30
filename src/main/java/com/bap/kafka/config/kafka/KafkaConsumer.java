package com.bap.kafka.config.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Optional;

/**
 * @author: heliang
 * @email heliang3019@163.com
 * @date: 2021/4/20 14:15
 */
@Component
public class KafkaConsumer {
    private static final Logger log = LoggerFactory.getLogger(KafkaConsumer.class);
    private static final String TPOIC = "topic02_v1";


    @KafkaListener(topics = {"inner_test_v1"}, containerFactory = "kafkaListenerContainerFactory")
    public void innerlistener(ConsumerRecord<?, ?> record) {
        log.info("inner kafka receive #key=" + record.key() + "#value=" + record.value());
    }

    @KafkaListener(topics = {"out_test_v1"}, containerFactory = "kafkaListenerContainerFactoryOutSchedule")
    public void outListener(ConsumerRecord<?, ?> record) {
        log.info("out kafka receive #key=" + record.key() + "#value=" + record.value());
    }

    @KafkaListener(id = "id0_v1", topicPartitions = {@TopicPartition(topic = TPOIC, partitions = {"0"})})
    public void listenPartition0(List<ConsumerRecord<?, ?>> records) {
        log.info("Id0 Listener, Thread ID: " + Thread.currentThread().getId());
        log.info("Id0 records size " + records.size());
        for (ConsumerRecord<?, ?> record : records) {
            Optional<?> kafkaMessage = Optional.ofNullable(record.value());
            log.info("Received: " + record);
            if (kafkaMessage.isPresent()) {
                Object message = record.value();
                String topic = record.topic();
                log.info("p0 Received message={}", message);
            }
        }
    }

    @KafkaListener(id = "id1_v1", topicPartitions = {@TopicPartition(topic = TPOIC, partitions = {"1"})})
    public void listenPartition1(List<ConsumerRecord<?, ?>> records) {
        log.info("Id1 Listener, Thread ID: " + Thread.currentThread().getId());
        log.info("Id1 records size " + records.size());

        for (ConsumerRecord<?, ?> record : records) {
            Optional<?> kafkaMessage = Optional.ofNullable(record.value());
            log.info("Received: " + record);
            if (kafkaMessage.isPresent()) {
                Object message = record.value();
                String topic = record.topic();
                log.info("p1 Received message={}", message);
            }
        }
    }

    @KafkaListener(groupId = "consumerGroup", topics = "sync_v1")
    public void onMessage(ConsumerRecords<Object, ?> consumerRecords,Acknowledgment ack) {
        for (org.apache.kafka.common.TopicPartition topicPartition : consumerRecords.partitions()) {
            for (ConsumerRecord<Object, ?> consumerRecord : consumerRecords.records(topicPartition)) {
                System.out.println("消费时间：" + System.currentTimeMillis() + "  " + consumerRecord.value());
            }
        }
        ack.acknowledge();//直接提交offset

    }

    @KafkaListener(groupId = "consumerGroup2", topics = "async_v1")
    public void consume2(ConsumerRecords<Object, ?> consumerRecords, Acknowledgment ack) {
        for (org.apache.kafka.common.TopicPartition topicPartition : consumerRecords.partitions()) {
            for (ConsumerRecord<Object, ?> consumerRecord : consumerRecords.records(topicPartition)) {
                System.out.println("消费时间：" + System.currentTimeMillis() + "  " + consumerRecord.value());
            }
        }
        ack.acknowledge();//直接提交offset

    }


}
