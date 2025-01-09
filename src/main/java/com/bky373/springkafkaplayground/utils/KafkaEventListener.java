package com.bky373.springkafkaplayground.utils;

import org.springframework.kafka.event.KafkaEvent;
import org.springframework.stereotype.Component;

@Component
public class KafkaEventListener {

//    @EventListener(KafkaEvent.class)
    public void onEvent(KafkaEvent event) {
        System.out.println(event);
    }

}
