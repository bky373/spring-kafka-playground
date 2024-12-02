package com.bky373.springkafkaplayground.retry.blocking;

import static com.bky373.springkafkaplayground.retry.KafkaRetryConfig.BLOCKING_RETRY_TOPIC;

import java.net.SocketException;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class BatchBlockingRetryListener {

    private static final Logger log = LoggerFactory.getLogger(BatchBlockingRetryListener.class);

//    @KafkaListener(topics = BLOCKING_RETRY_TOPIC, groupId = "batch-blocking-retry-group", containerFactory = "batchKafkaListenerContainerFactory")
    public void listen(List<ConsumerRecord> records) throws SocketException {
        records.forEach(record -> {
            if (record.offset() % 2 == 0) {
                throw new NullPointerException("batch");
            } else {
                log.info("[Done] r: {}", record);
            }
        });
    }
}
