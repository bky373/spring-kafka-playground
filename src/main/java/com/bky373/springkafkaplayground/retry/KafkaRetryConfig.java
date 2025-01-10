package com.bky373.springkafkaplayground.retry;

import static com.bky373.springkafkaplayground.KafkaCommonConfig.BROKERS;
import static com.bky373.springkafkaplayground.KafkaCommonConfig.MAX_PARTITION;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaAdmin.NewTopics;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;

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
    public KafkaTemplate<String, String> kafkaTemplate() {
        return new KafkaTemplate<>(myProducerFactory());
    }

    @Bean
    public ProducerFactory<String, String> myProducerFactory() {
        return new DefaultKafkaProducerFactory<>(senderProps());
    }

    public Map<String, Object> senderProps() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 100);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 10);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return props;
    }

    @Bean
    public <T> ConcurrentKafkaListenerContainerFactory<String, T> blockRetryKafkaListenerContainerFactory() {
        var factory = new ConcurrentKafkaListenerContainerFactory<String, T>();
        factory.setConsumerFactory(new DefaultKafkaConsumerFactory<>(commonProperties(BROKERS)));
        factory.setBatchListener(true);
        factory.setReplyTemplate(kafkaTemplate());
        factory.setCommonErrorHandler(commonErrorHandler());
        return factory;
    }

    private Map<String, Object> commonProperties(String bootstrapServers) {
        var props = new HashMap<String, Object>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 15000);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100);
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 22 * 1024 * 1024);
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 500);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 50 * 1024 * 1024);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, StringDeserializer.class);
        props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, StringDeserializer.class);
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
        props.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, "false");
        return props;
    }

    public CommonErrorHandler commonErrorHandler() {
        return new DefaultErrorHandler(
                new DeadLetterPublishingRecoverer(kafkaTemplate(),
                                                  (record, exception) -> new TopicPartition(record.topic() + "-DLT", record.partition())),
                new FixedBackOff(0, 2));
    }

//    @Bean
//    KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, String>> kafkaListenerContainerFactory() {
//        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
//        factory.setConsumerFactory(consumerFactory());
//        factory.getContainerProperties()
//               .setDeliveryAttemptHeader(true);
//        return factory;
//    }

//    @Bean
//    KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, String>> batchKafkaListenerContainerFactory() {
//        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
//        factory.setConsumerFactory(consumerFactory());
//        factory.setCommonErrorHandler(errorHandler());
////        factory.setBatchListener(true);
//        return factory;
//    }

//    @Bean
//    public ConsumerFactory<String, String> consumerFactory() {
//        return new DefaultKafkaConsumerFactory<>(consumerConfigs());
//    }

//    @Bean
//    public Map<String, Object> consumerConfigs() {
//        Map<String, Object> props = new HashMap<>();
//        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaCommonConfig.BROKERS);
//        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, KafkaCommonConfig.AUTO_OFFSET_RESET);
//        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
//        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
//        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
//        return props;
//    }

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
