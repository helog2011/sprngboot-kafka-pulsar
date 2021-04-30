package com.bap.kafka.config.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;

import java.util.concurrent.ExecutionException;

/**
 * @author: heliang
 * @email heliang3019@163.com
 * @date: 2021/4/20 14:13
 */
@Component // 这个必须加入容器不然，不会执行
@EnableScheduling // 这里是为了测试加入定时调度
public class KafkaProducer {
    private static final Logger log = LoggerFactory.getLogger(KafkaProducer.class);

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;
    @Autowired
    private KafkaTemplate<String, String> kafkaOutTemplate;

    public ListenableFuture<SendResult<String, String>> send(String topic, String key, String json) {
        ListenableFuture<SendResult<String, String>> result = kafkaTemplate.send(topic, key, json);
        log.info("inner kafka send #topic=" + topic + "#key=" + key + "#json=" + json + "#推送成功===========");
        return result;
    }

    public ListenableFuture<SendResult<String, String>> sendOut(String topic, String key, String json) {
        ListenableFuture<SendResult<String, String>> result = kafkaOutTemplate.send(topic, key, json);
        log.info("out kafka send #topic=" + topic + "#key=" + key + "#json=" + json + "#推送成功===========");
        return result;
    }

    public void sendSync() throws ExecutionException, InterruptedException {
        for (int i = 0; i < 100; i++) {
            SendResult<String, String> result = kafkaTemplate.send("sync_v1", "瓜田李下 同步发送" + i).get();
            log.info("发送时间：" + System.currentTimeMillis() + "  " + result);
        }
    }

    public void sendAsync() {
        for (int i = 0; i < 100; i++) {
            kafkaTemplate.send("async_v1", "瓜田李下 异步发送" + i);
        }
    }
}
