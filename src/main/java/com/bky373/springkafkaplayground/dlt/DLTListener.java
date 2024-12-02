package com.bky373.springkafkaplayground.dlt;

import static com.bky373.springkafkaplayground.dlt.KafkaDLTConfig.DLT_MAIN_TOPIC;

import java.net.SocketException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.retrytopic.DltStrategy;
import org.springframework.stereotype.Component;

@Component
public class DLTListener {

    private static final Logger log = LoggerFactory.getLogger(DLTListener.class);

    @RetryableTopic(attempts = "1",
            autoStartDltHandler = "false",
            dltStrategy = DltStrategy.FAIL_ON_ERROR)
    @KafkaListener(topics = DLT_MAIN_TOPIC, groupId = "dlt-main-group")
    public void listen(ConsumerRecord record) throws SocketException {
//        if (record.offset() % 2 == 0) {
        throw new SocketException("batch");
//        } else {
//            log.info("[Done] r: {}", record);
//        }
    }

    @DltHandler
    public void listenToDlt(ConsumerRecord record) throws SocketException {
        throw new RuntimeException("dlt-retry");
    }
//    @DltHandler
//    public void handleDltMessage(ConsumerRecord record, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
//        log.warn("Event on dlt topic={}, record={}", topic, record);
//    }
}
