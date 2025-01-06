package com.bky373.springkafkaplayground.retry.non_block;

import static com.bky373.springkafkaplayground.retry.KafkaRetryConfig.RETRYABLE_ANNOTATION_DEFAULT;

import java.math.BigInteger;
import java.net.SocketException;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.listener.AbstractConsumerSeekAware;
import org.springframework.kafka.listener.ConsumerSeekAware.ConsumerSeekCallback;
import org.springframework.kafka.retrytopic.RetryTopicHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

@Component
public class RetryableTopicDefaultRetryListener extends AbstractConsumerSeekAware {

    private static final Logger log = LoggerFactory.getLogger(RetryableTopicDefaultRetryListener.class);

    @RetryableTopic
    @KafkaListener(
            id = RETRYABLE_ANNOTATION_DEFAULT,
            topics = RETRYABLE_ANNOTATION_DEFAULT
    )
    public void listen(ConsumerRecord<String, String> input,
                       @Header(name = RetryTopicHeaders.DEFAULT_HEADER_BACKOFF_TIMESTAMP, required = false) byte[] time,
                       @Header(name = RetryTopicHeaders.DEFAULT_HEADER_ATTEMPTS, required = false) Integer nonBlockingAttempts)
            throws SocketException {
        log.info("--- Received. input: {}, nonBlockingAttempts: {}, time: {}", input.value(), nonBlockingAttempts, new BigInteger(time).longValue());
        long value = Long.parseLong(input.value());
//        if (value % 2 == 0) {
            log.warn("Force to throw error: {}", input.value());
            throw new SocketException(RETRYABLE_ANNOTATION_DEFAULT);
//        }
//        log.info("--------- Successfully processed. input: {}", input.value());
    }

    @Override
    public void onPartitionsAssigned(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
        assignments.forEach((tp, o) -> callback.seekToEnd(tp.topic(), tp.partition()));
    }
}
