package com.bky373.springkafkaplayground.retry;

import static com.bky373.springkafkaplayground.retry.KafkaRetryConfig.KAFKA_LISTENER_DEFAULT_RETRY;

import java.net.SocketException;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.listener.AbstractConsumerSeekAware;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

@Component
public class DefaultKafkaListenerRetryListener extends AbstractConsumerSeekAware {

    private static final Logger log = LoggerFactory.getLogger(DefaultKafkaListenerRetryListener.class);

    @RetryableTopic
    @KafkaListener(
            id = KAFKA_LISTENER_DEFAULT_RETRY,
            topics = KAFKA_LISTENER_DEFAULT_RETRY
    )
    public void listen(ConsumerRecord<String, String> input, @Header(KafkaHeaders.DELIVERY_ATTEMPT) int blockingAttempts) throws SocketException {
        log.info("--- Received. input: {}, attempt: {}", input.value(), blockingAttempts);
//        long value = Long.parseLong(input.value());
//        if ((value + blockingAttempts) % 2 == 0) {
        throw new SocketException(KAFKA_LISTENER_DEFAULT_RETRY);
//        }
//        log.info("--------- Successfully processed. input: {}", input.value());
    }

    @Override
    public void onPartitionsAssigned(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
        assignments.forEach((tp, o) -> callback.seekToEnd(tp.topic(), tp.partition()));
    }
}
