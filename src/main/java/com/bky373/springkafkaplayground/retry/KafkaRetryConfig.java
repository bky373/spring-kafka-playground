package com.bky373.springkafkaplayground.retry;

import static com.bky373.springkafkaplayground.KafkaCommonConfig.MAX_PARTITION;

import com.bky373.springkafkaplayground.KafkaCommonConfig;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaAdmin.NewTopics;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;

@Configuration
public class KafkaRetryConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaRetryConfig.class);
    private static final long RETRY_INTERVAL = 3000;
    private static final long RETRY_MAX_ATTEMPTS = 3;

    public static final String KAFKA_LISTENER_DEFAULT = "kafka-listener-default";
    public static final String RETRYABLE_ANNOTATION_DEFAULT = "retryable-annotation-default";

    @Bean
    public NewTopics topics() {
        return new NewTopics(
//                new NewTopic(KAFKA_LISTENER_DEFAULT, 1, (short) 1),
                new NewTopic(RETRYABLE_ANNOTATION_DEFAULT, MAX_PARTITION, (short) 1)
        );
    }

    @Bean
    KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, String>> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        factory.getContainerProperties()
               .setDeliveryAttemptHeader(true);
        return factory;
    }

//    @Bean
//    KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, String>> batchKafkaListenerContainerFactory() {
//        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
//        factory.setConsumerFactory(consumerFactory());
//        factory.setCommonErrorHandler(errorHandler());
////        factory.setBatchListener(true);
//        return factory;
//    }

    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        return new DefaultKafkaConsumerFactory<>(consumerConfigs());
    }

    @Bean
    public Map<String, Object> consumerConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaCommonConfig.BROKERS);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, KafkaCommonConfig.AUTO_OFFSET_RESET);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return props;
    }

//    @Bean
//    public DefaultErrorHandler errorHandler() {
//        BackOff backOff = new FixedBackOff(RETRY_INTERVAL, RETRY_MAX_ATTEMPTS);
//        DefaultErrorHandler errorHandler = new DefaultErrorHandler((consumerRecord, e) -> {
//            log.warn("[Retry] r: {}", consumerRecord, e);
//        }, backOff);
//        errorHandler.addRetryableExceptions(SocketException.class);
//        errorHandler.addNotRetryableExceptions(NullPointerException.class);
//        return errorHandler;
//    }
}
