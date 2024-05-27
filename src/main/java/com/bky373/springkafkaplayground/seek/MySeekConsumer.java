package com.bky373.springkafkaplayground.seek;

import java.util.List;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.AbstractConsumerSeekAware;
import org.springframework.stereotype.Component;

@Component
public class MySeekConsumer extends AbstractConsumerSeekAware {

    @KafkaListener(
            groupId = SeekConstants.GROUP_ID,
            topics = SeekConstants.TOPIC,
            containerFactory = "stringConsumerContainerFactory"
    )
    public void listen(List<String> events) {
        System.out.println("in = " + events);
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
