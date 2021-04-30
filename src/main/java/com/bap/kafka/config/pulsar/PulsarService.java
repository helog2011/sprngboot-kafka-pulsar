package com.bap.kafka.config.pulsar;

import com.bap.kafka.context.PulsarContext;
import com.google.gson.Gson;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.impl.BatchMessageIdImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.concurrent.CompletableFuture;

/**
 * @author: heliang
 * @email heliang3019@163.com
 * @date: 2021/4/25 14:12
 */
@Service
public class PulsarService {
    private static final Logger log = LoggerFactory.getLogger(PulsarService.class);
    private PulsarClient pulsarClient;
    private Gson gson;

    @Value("${pulsarServiceUrl}")
    private String pulsarServiceUrl;

    @Autowired
    private PulsarContext context;

    @PostConstruct
    public void init() {
        gson = new Gson();
        try {
            pulsarClient = PulsarClient.builder()
                    .serviceUrl(pulsarServiceUrl)
                    .build();
        } catch (PulsarClientException e) {
            log.error("PulsarClient build failure!! error={}", e.getMessage());
        }
    }

    /**
     * produce
     */
    public String produce(String topic, String message) throws PulsarClientException {
        Producer<String> producer = null;
        try {
            producer = pulsarClient.newProducer(Schema.STRING)
                    .topic("persistent://public/default/" + topic)
                    .create();
            MessageId messageId = producer.send(message);
            producer.close();
            log.info("pulsar->生产数据：topic={} message={} messageId={}", topic, message, messageId);
            return "topic=" + topic + " message=" + message + " messageId=" + messageId;
        } catch (Exception e) {
            log.error("Pulsar produce failure!! error={}", e.getMessage());
            if (null != producer) {
                producer.close();
            }
            return "Pulsar produce failure!! error=" + e.getMessage();
        }
    }

    /**
     * produce
     */
    public String sendAsync(String topic, String message) throws PulsarClientException {
        Producer<String> producer = null;
        try {
            producer = pulsarClient.newProducer(Schema.STRING).topic("persistent://public/default/" + topic).create();
            CompletableFuture<MessageId> completableFuture = producer.sendAsync(message);
            MessageId messageId = completableFuture.get();
            producer.closeAsync();
            log.info("pulsar->生产数据：topic={} message={} messageId={}", topic, message, messageId);
            return "topic=" + topic + " message=" + message + " messageId=" + messageId;
        } catch (Exception e) {
            log.error("Pulsar produce failure!! error={}", e.getMessage());
            if (null != producer) {
                producer.closeAsync();
            }
            return "Pulsar produce failure!! error=" + e.getMessage();
        }
    }

    /**
     * read
     */
    public void read(String topic, String offset) {
        new Thread(() -> {
            try {
                String[] offsetSplit = offset.split(":");
                MessageId msgId = new BatchMessageIdImpl(Long.parseLong(offsetSplit[0]), Long.parseLong(offsetSplit[1]), Integer.parseInt(offsetSplit[2]), Integer.parseInt(offsetSplit[3]));
                Reader<String> reader = pulsarClient.newReader(Schema.STRING)
                        .topic(topic)
                        .startMessageId(msgId)
                        .create();
                while (!Thread.currentThread().isInterrupted()) {
                    Message message = reader.readNext();
                    String data = new String(message.getData());
                    MessageId messageId = message.getMessageId();
                    log.info("pulsar->数据读取：topic={},message={},messageId={}", topic, data, messageId.toString());
                    Thread.sleep(20);
                }
            } catch (Exception e) {
                log.error("Pulsar read failure!! error={}", e.getMessage());

            }
        }).start();
    }

    /**
     * consume
     */
    public void consume(String topic) {
        new Thread(() -> {
            Consumer<String> consumer = null;
            MessageId messageId = null;
            try {
                consumer = pulsarClient.newConsumer(Schema.STRING)
                        .topic(topic)
                        .subscriptionName(topic)
                        .subscribe();
                while (!Thread.currentThread().isInterrupted()) {
                    Message<String> message = consumer.receive();
                    if (null != message.getData()) {
                        String data = new String(message.getData());
                        messageId = message.getMessageId();
                        if (context.getIPulsarServiceMap(topic).consume(messageId.toString(), data)) {
                            consumer.acknowledge(messageId);
//                            consumer.acknowledgeAsync(messageId)
//                                    .thenAccept(consumer -> successHandlerMethod())
//                                    .exceptionally(exception -> failHandlerMethod());
                        } else {
                            consumer.negativeAcknowledge(messageId);
                        }
                        log.info("pulsar->消费数据：topic={},message={},messageId={}", topic, data, messageId.toString());
                        Thread.sleep(20);
                    }
                }
            } catch (Exception e) {
                log.error("Pulsar consume failure!! error={}", e.getMessage());
                if (null != consumer) {
                    try {
                        consumer.negativeAcknowledge(messageId);
                        consumer.close();
                    } catch (PulsarClientException pulsarClientException) {
                        pulsarClientException.printStackTrace();
                    }
                }
            }
        }).start();
    }

    /**
     * 异步
     *
     * @param topic
     * @author heliang
     * @date 2021/4/25 16:41
     */
    public void consumeAsync(String topic) {
        new Thread(() -> {
            Consumer<String> consumer = null;
            MessageId messageId = null;
            try {
                consumer = pulsarClient.newConsumer(Schema.STRING)
                        .topic(topic)
                        .subscriptionName(topic)
                        .subscribe();
                while (!Thread.currentThread().isInterrupted()) {
                    CompletableFuture<Message<String>> completableFuture = consumer.receiveAsync();
                    Message<String> message = completableFuture.get();
                    if (null != message.getData()) {
                        String data = new String(message.getData());
                        messageId = message.getMessageId();
                        log.info("pulsar->消费数据：topic={},message={},messageId={}", topic, data, messageId.toString());
                        if (context.getIPulsarServiceMap(topic).consumeAsync(messageId.toString(), data)) {
                            consumer.acknowledgeAsync(messageId);
                        } else {
                            consumer.negativeAcknowledge(messageId);
                        }
                        Thread.sleep(20);
                    }
                }
            } catch (Exception e) {
                log.error("Pulsar consume failure!! error={}", e.getMessage());
                if (null != consumer) {
                    try {
                        consumer.negativeAcknowledge(messageId);
                        consumer.close();
                    } catch (PulsarClientException pulsarClientException) {
                        pulsarClientException.printStackTrace();
                    }
                }
            }
        }).start();
    }

}
