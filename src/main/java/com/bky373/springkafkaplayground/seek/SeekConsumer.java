package com.bky373.springkafkaplayground.seek;

import static com.bky373.springkafkaplayground.seek.SeekConstants.TOPIC_2;

import com.bky373.springkafkaplayground.ThreadSupport;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.AbstractConsumerSeekAware;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

@Component
public class SeekConsumer extends AbstractConsumerSeekAware {

    private Set<String> seekTopics = new HashSet<>();

    public void addSeekTopics(Set<String> seekTopics) {
        this.seekTopics.addAll(seekTopics);
    }

    @KafkaListener(
            groupId = SeekConstants.GROUP_ID,
            topics = SeekConstants.TOPIC_1,
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

    @KafkaListener(
            groupId = SeekConstants.GROUP_ID,
            topics = TOPIC_2,
            concurrency = "3",
            containerFactory = "stringConsumerContainerFactory"
    )
    public void listen2(@Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                        @Header(KafkaHeaders.OFFSET) long offset,
                        List<String> events) {
        System.out.printf("""
                                  [in-2] [%s] partition=%s, offset=%s, value=%s%n""",
                          ThreadSupport.getName(), partition, offset, events);
    }

    @Override
    public void seekToTimestamp(long time) {
        super.getCallbacksAndTopics()
             .forEach((cb, topicPartitions) -> {
                 if (!seekTopics.isEmpty()  && topicPartitions.stream()
                                                         .anyMatch(tp -> seekTopics.contains(tp.topic()))) {
                     System.out.println("topicPartitions = " + topicPartitions);
                     cb.seekToTimestamp(topicPartitions, time);
                 }
             });
    }
}
