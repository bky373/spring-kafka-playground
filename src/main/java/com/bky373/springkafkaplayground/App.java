package com.bky373.springkafkaplayground;

import static com.bky373.springkafkaplayground.retry.KafkaRetryConfig.DEFAULT_KAFKA_LISTENER_RETRY;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.KafkaTemplate;

@SpringBootApplication
public class App {

    private static final String TOPIC = DEFAULT_KAFKA_LISTENER_RETRY;
    private static final Logger log = LoggerFactory.getLogger(App.class);

    public static void main(String[] args) {
        SpringApplication.run(App.class, args);
    }

    @Bean
    public ApplicationRunner runner(KafkaTemplate<String, String> kafkaTemplate) {
        return args -> {
            Thread.sleep(5000);
            long now = System.currentTimeMillis();
            kafkaTemplate.send(TOPIC, String.valueOf(now), now + "");
            now = System.currentTimeMillis();
            kafkaTemplate.send(TOPIC, String.valueOf(now), now + "");
            log.info("Sending is Successful");
        };
    }
}
