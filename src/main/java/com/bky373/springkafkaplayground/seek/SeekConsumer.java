package com.bky373.springkafkaplayground.seek;

import com.bky373.springkafkaplayground.ThreadSupport;
import java.util.List;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.AbstractConsumerSeekAware;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

@Component
public class SeekConsumer extends AbstractConsumerSeekAware {

    @KafkaListener(
            groupId = SeekConstants.GROUP_ID,
            topics = SeekConstants.TOPIC,
            concurrency = "3",
            containerFactory = "stringConsumerContainerFactory"
    )
    public void listen(@Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                       @Header(KafkaHeaders.OFFSET) long offset,
                       List<String> events) {
        System.out.printf("""
                                [in] [%s] partition=%s, offset=%s, value=%s%n""",
                          ThreadSupport.getName(), partition, offset, events);
    }

//    @Override
//    public void registerSeekCallback(ConsumerSeekCallback callback) {
//        super.registerSeekCallback(new ConsumerSeekCallback() {
//            @Override
//            public void seek(String s, int i, long l) {
//
//            }
//
//            @Override
//            public void seekToBeginning(String s, int i) {
//
//            }
//
//            @Override
//            public void seekToEnd(String s, int i) {
//
//            }
//
//            @Override
//            public void seekRelative(String s, int i, long l, boolean b) {
//
//            }
//
//            @Override
//            public void seekToTimestamp(String s, int i, long l) {
//
//            }
//
//            @Override
//            public void seekToTimestamp(Collection<TopicPartition> collection, long l) {
//
//            }
//        });
//    }
}
