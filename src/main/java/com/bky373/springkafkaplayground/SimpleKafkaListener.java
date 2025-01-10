package com.bky373.springkafkaplayground;

import static com.bky373.springkafkaplayground.KafkaCommonConfig.MY_TOPIC;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

import java.util.List;

//@Component
public class SimpleKafkaListener {

//    @KafkaListener(id = "send-to", topics = MY_TOPIC, batch = "true")
    public List<Message<Object>> listen(List<ConsumerRecord> input) {
        try {
            System.out.println("Received messages.: " + input.getFirst()
                                                            .value());
            return input.stream()
                        .map(i -> MessageBuilder.withPayload(i.value())
                                                .setHeader(KafkaHeaders.PARTITION, i.partition())
                                                .setHeader(KafkaHeaders.KEY, i.key())
                                                .build())
                        .toList();
        } catch (Exception e) {
            System.out.println("Failed to process messages: " + input);
            return null;
        }
    }

//    @KafkaListener(id = "send-to-retry", topics = "my-topic-retry", batch = "true")
    public void listenRetry(List<ConsumerRecord> input) {
        System.out.println("[Retry] Received Message in group foo: " + input);
    }
}
