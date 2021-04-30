package com.bap.kafka.service.impl;

import com.bap.kafka.config.pulsar.PulsarService;
import com.bap.kafka.service.IPulsarService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * @author: heliang
 * @email heliang3019@163.com
 * @date: 2021/4/25 16:56
 */
@Service("topic1")
public class Topic1PulsarServiceImpl implements IPulsarService {
    private static final Logger log = LoggerFactory.getLogger(Topic1PulsarServiceImpl.class);

    @Override
    public Boolean consume(String messageId,String data) {
        log.info("topic1 consume 消费成功!");
        return true;
    }

    @Override
    public Boolean consumeAsync(String messageId,String data) {
        log.info("topic1 consumeAsync 消费成功!");
        return true;
    }
}
