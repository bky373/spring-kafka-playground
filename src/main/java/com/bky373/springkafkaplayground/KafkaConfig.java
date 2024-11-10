package com.bky373.springkafkaplayground;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

import java.util.HashMap;
import java.util.Map;

@EnableKafka
@Configuration
public class KafkaConfig {

    public static final String GROUP_ID = "GRP";
    public static final String TOPIC_1 = "TOPIC_1";
    public static final String TOPIC_2 = "TOPIC_2";

    @Value("${spring.kafka.bootstrap-servers:localhost:29092}")
    private String broker;

    @Bean
    public NewTopic topic1() {
        return new NewTopic(TOPIC_1, 1, (short) 1);
    }

//    @Bean
    public NewTopic topic2() {
        return new NewTopic(TOPIC_2, 3, (short) 2);
    }

//    @Bean
//    KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, String>> kafkaListenerContainerFactory() {
//        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
//        factory.setConsumerFactory(consumerFactory());
//        var props = factory.getContainerProperties();
//        props.setPollTimeout(3000);
//        props.setIdleEventInterval(3000L); // for ListenerContainerIdleEvent
//        return factory;
//    }

//    @Bean
//    public ConsumerFactory<String, String> consumerFactory() {
//        return new DefaultKafkaConsumerFactory<>(consumerConfigs());
//    }

    @Bean
    ConcurrentKafkaListenerContainerFactory<String, String> stringConsumerContainerFactory() {
        var factory = new ConcurrentKafkaListenerContainerFactory<String, String>();
        factory.setConsumerFactory(new DefaultKafkaConsumerFactory<>(consumerConfigs(),
                                                                     new StringDeserializer(),
                                                                     new StringDeserializer()));
//        factory.setBatchListener(true);
        return factory;
    }

    @Bean
    public Map<String, Object> consumerConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return props;
    }
}
