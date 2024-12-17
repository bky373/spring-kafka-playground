package com.bky373.springkafkaplayground.retry;

import com.bky373.springkafkaplayground.KafkaConfig;
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
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.ContainerProperties.AckMode;

@Configuration
public class KafkaRetryConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaRetryConfig.class);
    private static final long RETRY_INTERVAL = 3000;
    private static final long RETRY_MAX_ATTEMPTS = 3;

    public static final String BLOCKING_RETRY_TOPIC = "blocking-retry";
    public static final String NON_BLOCKING_RETRY_TOPIC = "non-blocking-retry";
    public static final String DEFAULT_KAFKA_LISTENER_RETRY = "default-kafka-listener-retry";
    public static final String RETRYABLE_ANNOTATION_DEFAULT_TOPIC = "retryable-annotation-default";

    @Bean
    public NewTopic blockingRetryTopic() {
        return new NewTopic(BLOCKING_RETRY_TOPIC, 1, (short) 1);
    }

    @Bean
    public NewTopics topics() {
        return new NewTopics(
                new NewTopic(DEFAULT_KAFKA_LISTENER_RETRY, 1, (short) 1)
        );
    }

    @Bean
    public NewTopic nonBlockingRetryTopic() {
        return new NewTopic(NON_BLOCKING_RETRY_TOPIC, 2, (short) 1);
    }

    @Bean
    KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, String>> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        factory.getContainerProperties().setDeliveryAttemptHeader(true);
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
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfig.BROKERS);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, KafkaConfig.AUTO_OFFSET_RESET);
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
