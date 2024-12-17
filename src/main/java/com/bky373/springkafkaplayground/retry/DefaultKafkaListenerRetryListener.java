package com.bky373.springkafkaplayground.retry;

import static com.bky373.springkafkaplayground.retry.KafkaRetryConfig.DEFAULT_KAFKA_LISTENER_RETRY;

import java.net.SocketException;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.AbstractConsumerSeekAware;
import org.springframework.stereotype.Component;

@Component
public class DefaultKafkaListenerRetryListener extends AbstractConsumerSeekAware {

    private static final Logger log = LoggerFactory.getLogger(DefaultKafkaListenerRetryListener.class);

    @KafkaListener(
            id = DEFAULT_KAFKA_LISTENER_RETRY,
            topics = DEFAULT_KAFKA_LISTENER_RETRY
    )
    public void listen(ConsumerRecord<String, String> input) throws SocketException {
        log.info("------ Received. input: {}", input.value());
        long value = Long.parseLong(input.value());
        if (value % 2 == 0) {
            throw new SocketException(DEFAULT_KAFKA_LISTENER_RETRY);
        }
        log.info("--------- Successfully processed. input: {}", input.value());
    }

    @Override
    public void onPartitionsAssigned(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
        assignments.forEach((tp, o) -> callback.seekToEnd(tp.topic(), tp.partition()));
    }
}
