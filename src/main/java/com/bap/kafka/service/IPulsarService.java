package com.bap.kafka.service;

public interface IPulsarService {

    Boolean consume(String messageId, String data);

    Boolean consumeAsync(String messageId, String data);
}
