package com.bky373.springkafkaplayground.seek;

import static java.util.concurrent.TimeUnit.SECONDS;

import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeoutException;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.event.ConsumerPausedEvent;
import org.springframework.kafka.event.ConsumerResumedEvent;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping
public class SeekController {

    private final KafkaListenerEndpointRegistry registry;
    private final SeekConsumer consumer;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private Semaphore paused = new Semaphore(0);


    public SeekController(KafkaListenerEndpointRegistry registry,
                          SeekConsumer consumer,
                          KafkaTemplate<String, String> kafkaTemplate) {
        this.registry = registry;
        this.consumer = consumer;
        this.kafkaTemplate = kafkaTemplate;
    }

    @GetMapping("/seek")
    void seek() throws InterruptedException, ExecutionException, TimeoutException {
        String topic = SeekConstants.TOPIC;
        int partition = 2;
//        TopicPartition tp = new TopicPartition(topic, partition);
        send("init", 0);
        send("init", 1);
        sendAndGet("init", 2);
        MessageListenerContainer container = registry.getAllListenerContainers()
                                                     .stream()
                                                     .filter(c -> Objects.equals(c.getGroupId(), SeekConstants.GROUP_ID))
                                                     .findFirst()
                                                     .get();
        container.pause();
        boolean paused = this.paused.tryAcquire(10, SECONDS);
        send("after pause", 0);
        send("after pause", 1);
        sendAndGet("after pause", 2);
        if (!paused) {
            System.out.println("failed");
            container.resume();
            return;
        }
        send("before now", 0);
        send("before now", 1);
        sendAndGet("before now", 2);
        long now = System.currentTimeMillis();
        send("after now", 0);
        send("after now", 1);
        sendAndGet("after now", 2);

        consumer.seekToTimestamp(now);
        container.resume();
        send("waiting pollTimeout", 0);
        send("waiting pollTimeout", 1);
        sendAndGet("waiting pollTimeout", 2);
        Thread.sleep(6000); // wait for at least one pollTimeout

        send("after wait pollTimeout", 0);
        send("after wait pollTimeout", 1);
        sendAndGet("after wait pollTimeout", 2);
        Thread.sleep(5000);
        send("end", 0);
        send("end", 1);
        sendAndGet("end", 2);
    }

    private void send(String value, int partition) {
        kafkaTemplate.send(SeekConstants.TOPIC, partition, String.valueOf(partition), value + ":" + partition);
    }

    private void sendAndGet(String value, int partition) throws InterruptedException, ExecutionException, TimeoutException {
        kafkaTemplate.send(SeekConstants.TOPIC, partition, String.valueOf(partition), value + ":" + partition)
                     .get(10, SECONDS);
    }

    @EventListener
    public void onConsumerPausedEvent(ConsumerPausedEvent event) {
        this.paused.release();
//        System.out.println(event);
    }

    @EventListener
    public void onConsumerResumedEvent(ConsumerResumedEvent event) {
//        System.out.println(event);
    }
}
