//package com.bky373.springkafkaplayground.retry.nonblocking;
//
//import static com.bky373.springkafkaplayground.retry.KafkaRetryConfig.NON_BLOCKING_RETRY_TOPIC;
//
//import org.apache.kafka.clients.consumer.ConsumerRecord;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import org.springframework.kafka.annotation.KafkaListener;
//import org.springframework.kafka.support.KafkaUtils;
//import org.springframework.stereotype.Component;
//
//import java.net.SocketException;
//
//@Component
//public class NonBlockingRetryListener {
//
//    private static final Logger log = LoggerFactory.getLogger(NonBlockingRetryListener.class);
//
//    @KafkaListener(
//            id = "non-block-retry",
//            topics = NON_BLOCKING_RETRY_TOPIC,
//            groupId = "non-blocking-retry-group",
//            properties = "auto.offset.reset=earliest"
//    )
//    public void listen(ConsumerRecord input) throws SocketException {
//        if (input.offset() % 2 == 0) {
//            throw new SocketException("non-blocking-retry main-topic failed");
//        } else {
//            log.info("[{} Received] input: {}", KafkaUtils.getConsumerGroupId(), input);
//        }
//    }
//}
