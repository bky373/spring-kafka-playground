package com.bky373.springkafkaplayground.seek;

import static com.bky373.springkafkaplayground.KafkaConfig.TOPIC_1;
import static java.util.concurrent.TimeUnit.SECONDS;

import java.time.LocalDateTime;
import java.util.Set;
import org.springframework.boot.actuate.endpoint.web.annotation.RestControllerEndpoint;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.event.ConsumerPartitionPausedEvent;
import org.springframework.kafka.event.ConsumerResumedEvent;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RestControllerEndpoint(id = "seek")
public class SeekController {

    private final SeekConsumer consumer;
    private final KafkaListenerEndpointRegistry registry;
    private final KafkaTemplate<String, String> kafkaTemplate;

    public SeekController(SeekConsumer consumer,
                          KafkaListenerEndpointRegistry registry,
                          KafkaTemplate<String, String> kafkaTemplate) {
        this.consumer = consumer;
        this.registry = registry;
        this.kafkaTemplate = kafkaTemplate;
    }

    @PostMapping
    public void seek(@RequestBody SeekRequest request) throws InterruptedException {
//         enforceRebalance 은 당연히 Consumer stop/start 를 하기 때문에 파티션 철회 / 재할당 등의 활동이 일어남.
//        MessageListenerContainer container = getContainer(SeekConstants.GROUP_ID);
//        container.enforceRebalance();
//        container.pausePartition(new TopicPartition(TOPIC_1, 0));
        send(TOPIC_1, "init", 0);
        Thread.sleep(1000);
//        send(TOPIC_1, "init", 0);
//        send(TOPIC_1, "init", 1);
//        send(TOPIC_1, "init", 2);
//        send(TOPIC_2, "init", 0);
//        send(TOPIC_2, "init", 1);
//        sendAndGet(TOPIC_2, "init", 2);
//        container.resumePartition(new TopicPartition(TOPIC_1, 0));
//        container.resume();
//        send(TOPIC_1, "before seek-time", 0);
//        send(TOPIC_1, "before seek-time", 1);
//        send(TOPIC_1, "before seek-time", 2);
//        send(TOPIC_2, "before seek-time", 0);
//        send(TOPIC_2, "before seek-time", 1);
//        send(TOPIC_2, "before seek-time", 2);

        // 아래 방식은 Consumer stop/start, 파티션 철회, 재할당 등의 활동이 일어나지 않음
        consumer.seekToBeginning();
//        consumer.seekToTimestamp(request.topics, DateTimeUtils.toTimestamp(request.seekAt));

//        send(TOPIC_1, "after seek-time", 0);
//        send(TOPIC_1, "after seek-time", 1);
//        send(TOPIC_1, "after seek-time", 2);
//        send(TOPIC_2, "after seek-time", 0);
//        send(TOPIC_2, "after seek-time", 1);
//        sendAndGet(TOPIC_2, "after seek-time", 2);

//        send(TOPIC_1, "end", 0);
//        send(TOPIC_1, "end", 1);
//        send(TOPIC_1, "end", 2);
//        send(TOPIC_2, "end", 0);
//        send(TOPIC_2, "end", 1);
//        sendAndGet(TOPIC_2, "end", 2);
    }

    private MessageListenerContainer getContainer(String groupId) {
        return registry.getAllListenerContainers()
                       .stream()
                       .filter(c -> groupId.equals(c.getGroupId()))
                       .findFirst()
                       .get();
    }

    private void send(String topic, String value, int partition) {
        kafkaTemplate.send(topic, partition, null, value + ":" + topic + ":" + partition);
    }

    private void sendAndGet(String topic, String value, int partition) {
        try {
            kafkaTemplate.send(topic, partition, null, value + ":" + partition)
                         .get(10, SECONDS);
        } catch (Exception e) {
            System.out.println("error occurred = " + e);
        }
    }


    @EventListener(ConsumerPartitionPausedEvent.class)
    public void onEvent(ConsumerPartitionPausedEvent event) {
        System.out.println("# [Event] " + event);
    }

    @EventListener(ConsumerResumedEvent.class)
    public void onEvent(ConsumerResumedEvent event) {
        System.out.println("# [Event] " + event);
    }


    public record SeekRequest(
            Set<String> topics,
            LocalDateTime seekAt
    ) {

    }
}
