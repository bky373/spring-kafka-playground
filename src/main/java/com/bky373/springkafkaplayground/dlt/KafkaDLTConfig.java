package com.bky373.springkafkaplayground.dlt;

import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.retrytopic.RetryTopicConfiguration;
import org.springframework.kafka.retrytopic.RetryTopicConfigurationBuilder;
import org.springframework.kafka.support.serializer.DeserializationException;

//@Configuration
public class KafkaDLTConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaDLTConfig.class);

    public static final String DLT_MAIN_TOPIC = "dtl-main-topic";

//    @Bean
//    public RetryTopicConfiguration myRetryTopic(KafkaTemplate<String, Object> template) {
//        return RetryTopicConfigurationBuilder
//                .newInstance()
//                .dltRoutingRules(Map.of("-deserialization", Set.of(DeserializationException.class)))
//                .create(template);
//    }
}
