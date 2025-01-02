package com.bky373.springkafkaplayground;

import static com.bky373.springkafkaplayground.KafkaCommonConfig.MAX_PARTITION;
import static com.bky373.springkafkaplayground.auto_commit.KafkaAutoCommitConfig.AUTO_COMMIT_TOPIC;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.Random;

@SpringBootApplication
public class App {

    private static final String TOPIC = AUTO_COMMIT_TOPIC;
    private static final Logger log = LoggerFactory.getLogger(App.class);

    public static void main(String[] args) {
        SpringApplication.run(App.class, args);
    }

    @Bean
    public ApplicationRunner runner(KafkaTemplate<String, String> kafkaTemplate) {
        return args -> {
            Random random = new Random();
            Thread.sleep(5000);
            for (int i = 0; i < 10; i++) {
                long now = System.currentTimeMillis();
                kafkaTemplate.send(TOPIC, random.nextInt(MAX_PARTITION), String.valueOf(now), now + "");
            }
            log.info("Sending is Successful");
        };
    }
}
