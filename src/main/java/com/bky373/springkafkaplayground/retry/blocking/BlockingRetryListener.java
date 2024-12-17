package com.bky373.springkafkaplayground.retry.blocking;

import java.net.SocketException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class BlockingRetryListener {

    private static final Logger log = LoggerFactory.getLogger(BlockingRetryListener.class);

//    @KafkaListener(topics = BLOCKING_RETRY_TOPIC, groupId = "blocking-retry-group")
    public void listen(ConsumerRecord record) throws SocketException {
        if (record.offset() % 2 == 0) {
            throw new SocketException("batch");
        } else {
            log.info("[Done] r: {}", record);
        }
    }
}
