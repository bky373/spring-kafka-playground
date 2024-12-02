package com.bky373.springkafkaplayground.dlt;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaDLTConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaDLTConfig.class);

    public static final String DLT_MAIN_TOPIC = "dtl-main-topic";
}
