package com.bky373.springkafkaplayground;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.AbstractConsumerSeekAware;
import org.springframework.stereotype.Component;

@SpringBootApplication
public class KafkaGh2302Application {

    public static void main(String[] args) {
        SpringApplication.run(KafkaGh2302Application.class, args);
    }


    @Bean
    public NewTopic topic() {
        return new NewTopic("seekExample", 3, (short) 1);
    }

    @Component
    public static class Listener extends AbstractConsumerSeekAware {

        @KafkaListener(id = "seekExample", topics = "seekExample", concurrency = "3")
        public void listen(String payload) {
            System.out.println("Listener received: " + payload);
        }

        @KafkaListener(id = "seekExample3", topics = "seekExample", concurrency = "3")
        public void listen3(String payload) {
            System.out.println("Listener3 received: " + payload);
        }

        public void seekToStart() {
            getSeekCallbacks().forEach((tp, callback) -> callback.seekToBeginning(tp.topic(), tp.partition()));
        }

    }

    @Component
    public static class Listener2 extends AbstractConsumerSeekAware {

        @KafkaListener(id = "seekExample2", topics = "seekExample", concurrency = "3")
        public void listen(String payload) {
            System.out.println("Listener2 received: " + payload);
        }

        public void seekToStart() {
            getSeekCallbacks().forEach((tp, callback) -> callback.seekToBeginning(tp.topic(), tp.partition()));
        }

    }

}