package com.bky373.springkafkaplayground.retry.block;

import static com.bky373.springkafkaplayground.KafkaCommonConfig.MY_TOPIC;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class BlockRetryListener {

    @KafkaListener(id = "block-retry", topics = MY_TOPIC, batch = "true", containerFactory = "blockRetryKafkaListenerContainerFactory")
    public void listen(List<ConsumerRecord> input) {
        input.stream()
             .forEach(i -> System.out.println("Received messages. " + i.value()));
        throw new RuntimeException("failed");
    }
}
