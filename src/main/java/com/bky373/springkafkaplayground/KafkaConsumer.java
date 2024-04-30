package com.bky373.springkafkaplayground;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class KafkaConsumer {

    @KafkaListener(id = "local1", topics = "topic1")
    public void consume(String in) {
        System.out.println("######## in = " + in);
    }
}
