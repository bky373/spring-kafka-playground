package com.bky373.springkafkaplayground;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;

@EnableKafka
@Configuration
public class KafkaCommonConfig {

    public static final String GROUP_ID = "my-group-id";
    public static final String MY_TOPIC = "my-topic";
    public static final String BROKERS = "localhost:29092";
    public static final String AUTO_OFFSET_RESET = "earliest";
    public static final int MAX_PARTITION = 1;

    @Bean
    public NewTopic myTopic() {
        return new NewTopic(MY_TOPIC, MAX_PARTITION, (short) 1);
    }
}
