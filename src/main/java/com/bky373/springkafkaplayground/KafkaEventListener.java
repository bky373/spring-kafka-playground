package com.bky373.springkafkaplayground;

import org.springframework.context.event.EventListener;
import org.springframework.kafka.event.KafkaEvent;
import org.springframework.stereotype.Component;

@Component
public class KafkaEventListener {

    @EventListener(KafkaEvent.class)
    public void onEvent(KafkaEvent event) {
        System.out.println("# [Event] " + event);
    }

}
