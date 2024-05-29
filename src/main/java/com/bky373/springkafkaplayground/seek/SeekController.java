package com.bky373.springkafkaplayground.seek;

import static com.bky373.springkafkaplayground.seek.SeekConstants.TOPIC_1;
import static com.bky373.springkafkaplayground.seek.SeekConstants.TOPIC_2;
import static java.util.concurrent.TimeUnit.SECONDS;

import java.util.Set;
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
        send(TOPIC_1, "init", 0);
        send(TOPIC_1, "init", 1);
        send(TOPIC_1, "init", 2);
        send(TOPIC_2, "init", 0);
        send(TOPIC_2, "init", 1);
        sendAndGet(TOPIC_2, "init", 2);

        send(TOPIC_1, "before seek-time", 0);
        send(TOPIC_1, "before seek-time", 1);
        send(TOPIC_1, "before seek-time", 2);
        send(TOPIC_2, "before seek-time", 0);
        send(TOPIC_2, "before seek-time", 1);
        send(TOPIC_2, "before seek-time", 2);
        long now = System.currentTimeMillis();

        send(TOPIC_1, "after seek-time", 0);
        send(TOPIC_1, "after seek-time", 1);
        send(TOPIC_1, "after seek-time", 2);
        send(TOPIC_2, "after seek-time", 0);
        send(TOPIC_2, "after seek-time", 1);
        sendAndGet(TOPIC_2, "after seek-time", 2);

        consumer.addSeekTopics(Set.of(TOPIC_1));
        consumer.seekToTimestamp(now);

        send(TOPIC_1, "after seeking", 0);
        send(TOPIC_1, "after seeking", 1);
        send(TOPIC_1, "after seeking", 2);
        send(TOPIC_2, "after seeking", 0);
        send(TOPIC_2, "after seeking", 1);
        sendAndGet(TOPIC_2, "after seeking", 2);

        send(TOPIC_1, "end", 0);
        send(TOPIC_1, "end", 1);
        send(TOPIC_1, "end", 2);
        send(TOPIC_2, "end", 0);
        send(TOPIC_2, "end", 1);
        sendAndGet(TOPIC_2, "end", 2);

        Thread.sleep(5000);
    }

    private void send(String topic, String value, int partition) {
        kafkaTemplate.send(topic, partition, null, value + ":" + partition);
    }

    private void sendAndGet(String topic, String value, int partition) throws InterruptedException, ExecutionException, TimeoutException {
        kafkaTemplate.send(topic, partition, null, value + ":" + partition)
                     .get(10, SECONDS);
    }
}
