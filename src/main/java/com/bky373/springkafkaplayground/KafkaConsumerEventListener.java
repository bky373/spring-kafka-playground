package com.bky373.springkafkaplayground;

import org.springframework.context.event.EventListener;
import org.springframework.kafka.event.ConsumerStartedEvent;
import org.springframework.kafka.event.ConsumerStartingEvent;
import org.springframework.kafka.event.ConsumerStoppedEvent;
import org.springframework.kafka.event.ConsumerStoppingEvent;
import org.springframework.stereotype.Component;

@Component
public class KafkaConsumerEventListener {

    @EventListener(ConsumerStartingEvent.class)
    public void onEvent(ConsumerStartingEvent event) {
        System.out.println("####### ConsumerStartingEvent = " + event);
    }

    @EventListener(ConsumerStartedEvent.class)
    public void onEvent(ConsumerStartedEvent event) {
        System.out.println("####### ConsumerStartedEvent = " + event);
    }

    @EventListener(ConsumerStoppingEvent.class)
    public void onEvent(ConsumerStoppingEvent event) {
        System.out.println("####### ConsumerStoppingEvent = " + event);
    }

    @EventListener(ConsumerStoppedEvent.class)
    public void onEvent(ConsumerStoppedEvent event) {
        System.out.println("####### ConsumerStoppedEvent = " + event);
    }

//    @EventListener(ListenerContainerIdleEvent.class)
//    public void onEvent(ListenerContainerIdleEvent event) {
//        System.out.println("####### ListenerContainerIdleEvent = " + event);
//    }
//
//    @EventListener(ListenerContainerNoLongerIdleEvent.class)
//    public void onEvent(ListenerContainerNoLongerIdleEvent event) {
//        System.out.println("####### ListenerContainerNoLongerIdleEvent = " + event);
//    }
}
