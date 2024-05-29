package com.bky373.springkafkaplayground.seek;

import static java.util.concurrent.TimeUnit.SECONDS;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping
public class SeekController {

    private final SeekConsumer consumer;
    private final KafkaTemplate<String, String> kafkaTemplate;

    public SeekController(SeekConsumer consumer,
                          KafkaTemplate<String, String> kafkaTemplate) {
        this.consumer = consumer;
        this.kafkaTemplate = kafkaTemplate;
    }

    @GetMapping("/seek")
    void seek() throws InterruptedException, ExecutionException, TimeoutException {
        send("init", 0);
        send("init", 1);
        sendAndGet("init", 2);

        send("before seek-time", 0);
        send("before seek-time", 1);
        sendAndGet("before seek-time", 2);

        long now = System.currentTimeMillis();

        send("after seek-time", 0);
        send("after seek-time", 1);
        sendAndGet("after seek-time", 2);

        consumer.seekToTimestamp(now);

        send("after seeking", 0);
        send("after seeking", 1);
        sendAndGet("after seeking", 2);

        send("end", 0);
        send("end", 1);
        sendAndGet("end", 2);

        Thread.sleep(5000);
    }

    private void send(String value, int partition) {
        kafkaTemplate.send(SeekConstants.TOPIC, partition, String.valueOf(partition), value + ":" + partition);
    }

    private void sendAndGet(String value, int partition) throws InterruptedException, ExecutionException, TimeoutException {
        kafkaTemplate.send(SeekConstants.TOPIC, partition, String.valueOf(partition), value + ":" + partition)
                     .get(10, SECONDS);
    }
}
