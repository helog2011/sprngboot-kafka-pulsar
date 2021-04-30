package com.bap.kafka.web;

import com.bap.kafka.config.pulsar.PulsarService;
import org.apache.pulsar.client.api.PulsarClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author: heliang
 * @email heliang3019@163.com
 * @date: 2021/4/25 14:13
 */
@RestController
public class PulsarController {
    private static final Logger log = LoggerFactory.getLogger(PulsarService.class);

    @Resource
    private PulsarService pulsarService;
    // 创建线程池
    private ExecutorService executorService = new ThreadPoolExecutor(5, 10, 10L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
    private AtomicInteger count = new AtomicInteger(0);
    //队列,拿任务的执行结果
    private BlockingQueue<Future<String>> queue = new LinkedBlockingQueue<>();

    private int size = 20;


    @GetMapping(value = "/pulsar/produce")
    public String produce(String topic, String message) throws  ExecutionException, InterruptedException {
        long start = System.currentTimeMillis();

        for (int i = 0; i < size; i++) {
            int a = i;
            log.info("----------------------------当前是遍历第{}次----------------------------", a);
            Future<String> future = executorService.submit(new Callable<String>() {
                @Override
                public String call() throws Exception {
                    log.info("----------------------------当前是生产消息第{}次----------------------------", a);
                    String msg = pulsarService.produce(topic, message + a);
                    log.info("PulsarClient produce {}", msg);
                    return msg;
                }
            });
        }
        // 检查线程池任务执行结果
        for (int i = 0; i < size; i++) {
            String result = queue.take().get();
            log.info("执行第{}次返回的结果", result);
        }
        // 关闭线程池
        executorService.shutdown();
        log.info("-------------tasks sleep time " + count.get()
                + "ms,and spend time "
                + (System.currentTimeMillis() - start) + " ms");
        return null;

    }


    @GetMapping(value = "/pulsar/produceAsync")
    public String produceAsync(String topic, String message) throws PulsarClientException, ExecutionException, InterruptedException {
        long start = System.currentTimeMillis();
        for (int i = 0; i < size; i++) {
            int a = i;
            log.info("----------------------------当前是遍历第{}次----------------------------", a);
            Future<String> future = executorService.submit(new Callable<String>() {
                @Override
                public String call() throws Exception {
                    log.info("----------------------------当前是生产消息第{}次----------------------------", a);
                    String msg = pulsarService.sendAsync(topic, message + a);
                    log.info("PulsarClient produce {}", msg);
                    return msg;
                }
            });
        }
        // 检查线程池任务执行结果
        for (int i = 0; i < size; i++) {
            String result = queue.take().get();
            log.info("执行第{}次返回的结果", result);
        }
        // 关闭线程池
        executorService.shutdown();
        log.info("-------------tasks sleep time " + count.get()
                + "ms,and spend time "
                + (System.currentTimeMillis() - start) + " ms");
        return null;

    }

    @GetMapping(value = "/pulsar/read")
    public String read(String topic, String offset) {
        pulsarService.read(topic, offset);
        return "SUCCESS";
    }

    @GetMapping(value = "/pulsar/consume")
    public String consume(String topic) {
        pulsarService.consume(topic);
        return "SUCCESS";
    }

    @GetMapping(value = "/pulsar/consumeAsync")
    public String consumeAsync(String topic) {
        pulsarService.consumeAsync(topic);
        return "SUCCESS";
    }
}
