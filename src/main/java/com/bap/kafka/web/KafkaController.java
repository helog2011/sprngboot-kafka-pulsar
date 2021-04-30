package com.bap.kafka.web;

import com.bap.kafka.config.kafka.KafkaProducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author: heliang
 * @email heliang3019@163.com
 * @date: 2021/4/25 14:14
 */
@RestController
public class KafkaController {
    @Autowired
    private KafkaProducer kafkaProducer;
    ExecutorService executorService = Executors.newSingleThreadExecutor();

    @GetMapping(value = "/kafka/produce")
    public String produce(String topic, String message) throws ExecutionException, InterruptedException {
        Set<Callable<String>> callables = new HashSet<Callable<String>>();
        for (int i = 0; i < 1000; i++) {
            int finalI1 = i;
            callables.add(new Callable<String>() {
                @Override
                public String call() throws Exception {
//                    kafkaProducer.send("inner_test", "douzi" + finalI1, "liyuehua" + finalI1);
//                    kafkaProducer.sendOut("out_test", "douziout" + finalI1, "fanbingbing" + finalI1);
                    kafkaProducer.sendAsync();
                    kafkaProducer.sendSync();
                    return "Task " + finalI1;
                }
            });
        }
        String result = executorService.invokeAny(callables);
        System.out.println("result = " + result);
        executorService.shutdown();
        return result;
    }
}
