package com.bap.kafka.config.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;

import java.util.HashMap;
import java.util.Map;

/**
 * @author: heliang
 * @email heliang3019@163.com
 * @date: 2021/4/20 14:05
 */
@Configuration
@EnableKafka
public class KafkaConfig {
    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;
    @Value("${spring.kafka.consumer.enable-auto-commit}")
    private String autoCommit;
    @Value("${spring.kafka.consumer.auto.commit-interval}")
    private Integer autoCommitInterval;
    @Value("${spring.kafka.consumer.group-id}")
    private String groupId;
    @Value("${spring.kafka.consumer.max-poll-records}")
    private Integer maxPollRecords;
    @Value("${spring.kafka.consumer.auto-offset-reset}")
    private String autoOffsetReset;
    @Value("${spring.kafka.producer.retries}")
    private Integer retries;
    @Value("${spring.kafka.producer.batch-size}")
    private Integer batchSize;
    @Value("${spring.kafka.producer.buffer-memory}")
    private Integer bufferMemory;



    @Bean
    @Primary
//理解为默认优先选择当前容器下的消费者工厂
    public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<Integer, String>> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<Integer, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        factory.setConcurrency(3);
        factory.setBatchListener(true);
        factory.getContainerProperties().setPollTimeout(3000);
        //配置手动提交offset
        factory.getContainerProperties().setAckMode((ContainerProperties.AckMode.MANUAL));
        return factory;
    }

    @Bean//第一个消费者工厂的bean
    public ConsumerFactory<Integer, String> consumerFactory() {
        return new DefaultKafkaConsumerFactory<>(consumerConfigs());
    }

    @Bean
    public Map<String, Object> consumerConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords);
        //禁止自动提交
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, autoCommit);
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 100);

        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 120000);
        props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 180000);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        //一个设启用批量消费，一个设置批量消费每次最多消费多少条消息记录
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);
        return props;
    }

    @Bean //生产者工厂配置
    public ProducerFactory<String, String> producerFactory() {
        return new DefaultKafkaProducerFactory<>(senderProps());
    }

    @Bean //kafka发送消息模板
    public KafkaTemplate<String, String> kafkaTemplate() {
        return new KafkaTemplate<String, String>(producerFactory());
    }

    /**
     * 生产者配置方法
     * <p>
     * 生产者有三个必选属性
     * <p>
     * 1.bootstrap.servers broker地址清单，清单不要包含所有的broker地址，
     * 生产者会从给定的broker里查找到其他broker的信息。不过建议至少提供两个broker信息，一旦 其中一个宕机，生产者仍能能够连接到集群上。
     * </p>
     * <p>
     * 2.key.serializer broker希望接收到的消息的键和值都是字节数组。 生产者用对应的类把键对象序列化成字节数组。
     * </p>
     * <p>
     * 3.value.serializer 值得序列化方式
     * </p>
     *
     * @return
     */
    private Map<String, Object> senderProps() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        /**
         * 当从broker接收到的是临时可恢复的异常时，生产者会向broker重发消息，但是不能无限
         * 制重发，如果重发次数达到限制值，生产者将不会重试并返回错误。
         * 通过retries属性设置。默认情况下生产者会在重试后等待100ms，可以通过 retries.backoff.ms属性进行修改
         */
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        /**
         * 在考虑完成请求之前，生产者要求leader收到的确认数量。这可以控制发送记录的持久性。允许以下设置：
         * <ul>
         * <li>
         * <code> acks = 0 </ code>如果设置为零，则生产者将不会等待来自服务器的任何确认。该记录将立即添加到套接字缓冲区并视为已发送。在这种情况下，无法保证服务器已收到记录，并且
         * <code>retries </ code>配置将不会生效（因为客户端通常不会知道任何故障）。为每条记录返回的偏移量始终设置为-1。
         * <li> <code> acks = 1 </code>
         * 这意味着leader会将记录写入其本地日志，但无需等待所有follower的完全确认即可做出回应。在这种情况下，
         * 如果leader在确认记录后立即失败但在关注者复制之前，则记录将丢失。
         * <li><code> acks = all </code>
         * 这意味着leader将等待完整的同步副本集以确认记录。这保证了只要至少一个同步副本仍然存活，记录就不会丢失。这是最强有力的保证。
         * 这相当于acks = -1设置
         */
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        /**
         * 当有多条消息要被发送到统一分区是，生产者会把他们放到统一批里。kafka通过批次的概念来 提高吞吐量，但是也会在增加延迟。
         */
        // 以下配置当缓存数量达到16kb，就会触发网络请求，发送消息
//        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        // 每条消息在缓存中的最长时间，如果超过这个时间就会忽略batch.size的限制，由客户端立即将消息发送出去
//        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);//设置kafka消息大小，msg超出设定大小无法发送到kafka
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return props;
    }



    @Value("${spring.outkafka.bootstrap-servers}")
    private String outBootstrapServers;
    @Value("${spring.outkafka.consumer.enable-auto-commit}")
    private String outAutoCommit;
    @Value("${spring.outkafka.consumer.auto.commit-interval}")
    private Integer outAutoCommitInterval;
    @Value("${spring.outkafka.consumer.group-id}")
    private String outGroupId;
    @Value("${spring.outkafka.consumer.max-poll-records}")
    private Integer outMaxPollRecords;
    @Value("${spring.outkafka.consumer.auto-offset-reset}")
    private String outAutoOffsetReset;
    @Value("${spring.outkafka.producer.retries}")
    private Integer outRetries;
    @Value("${spring.outkafka.producer.batch-size}")
    private Integer outBatchSize;
    @Value("${spring.outkafka.producer.buffer-memory}")
    private Integer outBufferMemory;


    static {

    }

    /**
     * 连接第二个kafka集群的配置
     */
    @Bean
    KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<Integer, String>> kafkaListenerContainerFactoryOutSchedule() {
        ConcurrentKafkaListenerContainerFactory<Integer, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactoryOutSchedule());
        factory.setConcurrency(3);
        // 设置为批量消费，每个批次数量在Kafka配置参数中设置ConsumerConfig.MAX_POLL_RECORDS_CONFIG
        factory.setBatchListener(true);
        factory.getContainerProperties().setPollTimeout(3000);
        factory.getContainerProperties().setAckMode((ContainerProperties.AckMode.MANUAL));
        return factory;
    }

    @Bean
    public ConsumerFactory<Integer, String> consumerFactoryOutSchedule() {
        return new DefaultKafkaConsumerFactory<>(consumerConfigsOutSchedule());
    }

    /**
     * 连接第二个集群的消费者配置
     */
    @Bean
    public Map<String, Object> consumerConfigsOutSchedule() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.ACKS_CONFIG, "0");//默认为1,all和-1都是消费在服务副本里 也已经接收成功，防止数据丢失
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, outBootstrapServers);
        props.put(ProducerConfig.RETRIES_CONFIG, outRetries);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, outBatchSize);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, bufferMemory);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);




        props.put(ConsumerConfig.GROUP_ID_CONFIG, outGroupId);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, outAutoCommit);
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "100");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "15000");

        //一个设启用批量消费，一个设置批量消费每次最多消费多少条消息记录
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);
        return props;
    }

    @Bean //生产者工厂配置
    public ProducerFactory<String, Object> producerOutFactory() {
        return new DefaultKafkaProducerFactory<>(senderOutProps());
    }

    @Bean //kafka发送消息模板
    public KafkaTemplate<String, Object> kafkaOutTemplate() {
        return new KafkaTemplate<String, Object>(producerOutFactory());
    }

    /**
     * 生产者配置方法
     * <p>
     * 生产者有三个必选属性
     * <p>
     * 1.bootstrap.servers broker地址清单，清单不要包含所有的broker地址，
     * 生产者会从给定的broker里查找到其他broker的信息。不过建议至少提供两个broker信息，一旦 其中一个宕机，生产者仍能能够连接到集群上。
     * </p>
     * <p>
     * 2.key.serializer broker希望接收到的消息的键和值都是字节数组。 生产者用对应的类把键对象序列化成字节数组。
     * </p>
     * <p>
     * 3.value.serializer 值得序列化方式
     * </p>
     *
     * @return
     */
    private Map<String, Object> senderOutProps() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, outBootstrapServers);
        /**
         * 当从broker接收到的是临时可恢复的异常时，生产者会向broker重发消息，但是不能无限
         * 制重发，如果重发次数达到限制值，生产者将不会重试并返回错误。
         * 通过retries属性设置。默认情况下生产者会在重试后等待100ms，可以通过 retries.backoff.ms属性进行修改
         */
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        /**
         * 在考虑完成请求之前，生产者要求leader收到的确认数量。这可以控制发送记录的持久性。允许以下设置：
         * <ul>
         * <li>
         * <code> acks = 0 </ code>如果设置为零，则生产者将不会等待来自服务器的任何确认。该记录将立即添加到套接字缓冲区并视为已发送。在这种情况下，无法保证服务器已收到记录，并且
         * <code>retries </ code>配置将不会生效（因为客户端通常不会知道任何故障）。为每条记录返回的偏移量始终设置为-1。
         * <li> <code> acks = 1 </code>
         * 这意味着leader会将记录写入其本地日志，但无需等待所有follower的完全确认即可做出回应。在这种情况下，
         * 如果leader在确认记录后立即失败但在关注者复制之前，则记录将丢失。
         * <li><code> acks = all </code>
         * 这意味着leader将等待完整的同步副本集以确认记录。这保证了只要至少一个同步副本仍然存活，记录就不会丢失。这是最强有力的保证。
         * 这相当于acks = -1设置
         */
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        /**
         * 当有多条消息要被发送到统一分区是，生产者会把他们放到统一批里。kafka通过批次的概念来 提高吞吐量，但是也会在增加延迟。
         */
        // 以下配置当缓存数量达到16kb，就会触发网络请求，发送消息
//        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        // 每条消息在缓存中的最长时间，如果超过这个时间就会忽略batch.size的限制，由客户端立即将消息发送出去
//        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
//        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return props;
    }
}
