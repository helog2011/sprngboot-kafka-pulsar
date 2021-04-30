//package com.bap.kafka;
//
//import com.bap.kafka.config.kafka.KafkaProducer;
//import org.junit.jupiter.api.Test;
//import org.junit.runner.RunWith;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.boot.test.context.SpringBootTest;
//import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
//
//import java.util.HashSet;
//import java.util.Set;
//import java.util.concurrent.Callable;
//import java.util.concurrent.ExecutionException;
//import java.util.concurrent.ExecutorService;
//import java.util.concurrent.Executors;
//
//@RunWith(SpringJUnit4ClassRunner.class)
//@SpringBootTest(classes = {SprngbootKafkaApplication.class})
//class SprngbootKafkaApplicationTests {
//
//    @Autowired
//    private KafkaProducer kafkaProducer;
//    ExecutorService executorService = Executors.newSingleThreadExecutor();
//
//
//    @Test
//    public void sendInner() throws ExecutionException, InterruptedException {
//        Set<Callable<String>> callables = new HashSet<Callable<String>>();
//        for (int i = 0; i < 1000; i++) {
//            int finalI1 = i;
//            callables.add(new Callable<String>() {
//                public String call() throws Exception {
////                    kafkaProducer.send("inner_test", "douzi" + finalI1, "liyuehua" + finalI1);
////                    kafkaProducer.sendOut("out_test", "douziout" + finalI1, "fanbingbing" + finalI1);
//                    kafkaProducer.sendAsync();
//                    kafkaProducer.sendSync();
//                    return "Task " + finalI1;
//                }
//            });
//        }
//        String result = executorService.invokeAny(callables);
//        System.out.println("result = " + result);
//        executorService.shutdown();
//    }
//}
