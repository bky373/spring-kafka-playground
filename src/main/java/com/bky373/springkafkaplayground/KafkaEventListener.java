//package com.bky373.springkafkaplayground;
//
//import org.springframework.context.ApplicationEvent;
//import org.springframework.context.event.EventListener;
//import org.springframework.kafka.event.ConsumerResumedEvent;
//import org.springframework.kafka.event.ConsumerStartedEvent;
//import org.springframework.kafka.event.ConsumerStartingEvent;
//import org.springframework.kafka.event.ConsumerStoppedEvent;
//import org.springframework.kafka.event.ConsumerStoppingEvent;
//import org.springframework.kafka.event.ListenerContainerIdleEvent;
//import org.springframework.kafka.event.ListenerContainerNoLongerIdleEvent;
//import org.springframework.stereotype.Component;
//
//@Component
//public class KafkaEventListener {
//
//    @EventListener(ConsumerStartingEvent.class)
//    public void onEvent(ConsumerStartingEvent event) {
//        printEvent(event);
//    }
//
//    @EventListener(ConsumerStartedEvent.class)
//    public void onEvent(ConsumerStartedEvent event) {
//        printEvent(event);
//    }
//
//    @EventListener(ConsumerStoppingEvent.class)
//    public void onEvent(ConsumerStoppingEvent event) {
//        printEvent(event);
//    }
//
//    @EventListener(ConsumerStoppedEvent.class)
//    public void onEvent(ConsumerStoppedEvent event) {
//        printEvent(event);
//    }
//
//    @EventListener(ConsumerResumedEvent.class)
//    public void onEvent(ConsumerResumedEvent event) {
//        printEvent(event);
//    }
//
//    @EventListener(ListenerContainerIdleEvent.class)
//    public void onEvent(ListenerContainerIdleEvent event) {
//        printEvent(event);
//    }
//
//    @EventListener(ListenerContainerNoLongerIdleEvent.class)
//    public void onEvent(ListenerContainerNoLongerIdleEvent event) {
//        printEvent(event);
//    }
//
//    public static void printEvent(ApplicationEvent event) {
//        System.out.println("# [EVENT] " + event);
//    }
//}
