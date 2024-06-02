package com.bky373.springkafkaplayground.seek;

import static com.bky373.springkafkaplayground.seek.SeekConstants.TOPIC_1;
import static java.util.concurrent.TimeUnit.SECONDS;

import com.bky373.springkafkaplayground.DateTimeUtils;
import java.time.LocalDateTime;
import java.util.Set;
import org.springframework.boot.actuate.endpoint.web.annotation.RestControllerEndpoint;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RestControllerEndpoint(id = "seek")
public class SeekController {

    private final SeekConsumer consumer;
    private final KafkaTemplate<String, String> kafkaTemplate;

    public SeekController(SeekConsumer consumer,
                          KafkaTemplate<String, String> kafkaTemplate) {
        this.consumer = consumer;
        this.kafkaTemplate = kafkaTemplate;
    }

    @PostMapping
    public void seek(@RequestBody SeekRequest request) {
        send(TOPIC_1, "init", 0);
        send(TOPIC_1, "init", 1);
        sendAndGet(TOPIC_1, "init", 2);

        send(TOPIC_1, "before seek-time", 0);
        send(TOPIC_1, "before seek-time", 1);
        sendAndGet(TOPIC_1, "before seek-time", 2);

        consumer.seekToTimestamp(request.topics, DateTimeUtils.toTimestamp(request.seekAt));

        send(TOPIC_1, "after seek-time", 0);
        send(TOPIC_1, "after seek-time", 1);
        sendAndGet(TOPIC_1, "after seek-time", 2);

        send(TOPIC_1, "end", 0);
        send(TOPIC_1, "end", 1);
        sendAndGet(TOPIC_1, "end", 2);
    }

    private void send(String topic, String value, int partition) {
        kafkaTemplate.send(topic, partition, null, value + ":" + partition);
    }

    private void sendAndGet(String topic, String value, int partition) {
        try {
            kafkaTemplate.send(topic, partition, null, value + ":" + partition)
                         .get(10, SECONDS);
        } catch (Exception e) {
            System.out.println("error occurred = " + e);
        }
    }

    public record SeekRequest(
            Set<String> topics,
            LocalDateTime seekAt
    ) {

    }
}
