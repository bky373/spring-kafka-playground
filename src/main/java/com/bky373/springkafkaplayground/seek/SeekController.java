package com.bky373.springkafkaplayground.seek;

import static java.util.concurrent.TimeUnit.SECONDS;

import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.common.TopicPartition;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.event.ConsumerPartitionPausedEvent;
import org.springframework.kafka.event.ConsumerPartitionResumedEvent;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping
public class SeekController {

    private final KafkaListenerEndpointRegistry registry;
    private final MySeekConsumer consumer;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private Semaphore paused = new Semaphore(0);


    public SeekController(KafkaListenerEndpointRegistry registry,
                          MySeekConsumer consumer,
                          KafkaTemplate<String, String> kafkaTemplate) {
        this.registry = registry;
        this.consumer = consumer;
        this.kafkaTemplate = kafkaTemplate;
    }

    @GetMapping("/seek")
    void seek() throws InterruptedException, ExecutionException, TimeoutException {
        String topic = SeekConstants.TOPIC;
        int partition = 0;
        TopicPartition tp = new TopicPartition(topic, partition);
        MessageListenerContainer container = registry.getAllListenerContainers()
                                                     .stream()
                                                     .filter(c -> Objects.equals(c.getGroupId(), SeekConstants.GROUP_ID))
                                                     .findFirst()
                                                     .get();
        send("init");
        container.pausePartition(tp);
        boolean paused = this.paused.tryAcquire(10, SECONDS);
        kafkaTemplate.send(SeekConstants.TOPIC, "after pause");
        if (!paused) {
            System.out.println("failed");
            container.resumePartition(tp);
            return;
        }
        send("before now");
        long now = System.currentTimeMillis();
        send("after now");

        consumer.seekToTimestamp(now);
        container.resumePartition(tp);
        Thread.sleep(6000); // wait for at least one pollTimeout

        send("after resume");
        Thread.sleep(5000);
        send("end");
    }

    private void send(String value) throws InterruptedException, ExecutionException, TimeoutException {
        kafkaTemplate.send(SeekConstants.TOPIC, value)
                     .get(10, SECONDS);
    }

    @EventListener
    public void onConsumerPausedEvent(ConsumerPartitionPausedEvent event) {
        this.paused.release();
        System.out.println(event);
    }

    @EventListener
    public void onConsumerResumedEvent(ConsumerPartitionResumedEvent event) {
        System.out.println(event);
    }
}
