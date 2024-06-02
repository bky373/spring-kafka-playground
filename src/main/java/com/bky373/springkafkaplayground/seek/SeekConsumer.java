package com.bky373.springkafkaplayground.seek;

import com.bky373.springkafkaplayground.ThreadSupport;
import java.util.List;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.AbstractConsumerSeekAware;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

@Component
public class SeekConsumer extends AbstractConsumerSeekAware {

    private static final Logger log = LoggerFactory.getLogger(SeekConsumer.class);

    @Override
    public void registerSeekCallback(ConsumerSeekCallback callback) {
        super.registerSeekCallback(callback);
    }

    @KafkaListener(
            groupId = SeekConstants.GROUP_ID_1,
            topics = {SeekConstants.TOPIC_1},
            concurrency = "3",
            containerFactory = "stringConsumerContainerFactory"
    )
    public void listen(@Header(KafkaHeaders.RECEIVED_TOPIC) Set<String> topics,
                       @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                       @Header(KafkaHeaders.OFFSET) long offset,
                       @Header(KafkaHeaders.GROUP_ID) String groupId,
                       List<String> events) {
        System.out.printf(String.format("[in] [%s] groupId=%s, topics=%s, partition=%s, offset=%s, value=%s%n)",
                                        ThreadSupport.getName(), groupId, topics, partition, offset, events));
    }

    @KafkaListener(
            groupId = SeekConstants.GROUP_ID_2,
            topics = SeekConstants.TOPIC_1,
            concurrency = "3",
            containerFactory = "stringConsumerContainerFactory"
    )
    public void listen2(@Header(KafkaHeaders.RECEIVED_TOPIC) Set<String> topics,
                        @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                        @Header(KafkaHeaders.OFFSET) long offset,
                        @Header(KafkaHeaders.GROUP_ID) String groupId,
                        List<String> events) {
        System.out.printf(String.format("[in] [%s] groupId=%s, topics=%s, partition=%s, offset=%s, value=%s%n)",
                                        ThreadSupport.getName(), groupId, topics, partition, offset, events));
    }

    /**
     * 모든 토픽이 아닌, 특정 토픽의 오프셋만 조정합니다.
     * 현재 시간 이후의 오프셋으로 조정하면, 조정하는 동안 발행된 이벤트가 누락되므로 시간 유효성을 검증합니다.
     */
    public void seekToTimestamp(Set<String> seekTopics, long time) {
        if (seekTopics.isEmpty() || time <= 0 || time >= System.currentTimeMillis()) {
            log.warn("[오프셋 조정 불가] seekTopics: {}, time: {}", seekTopics, time);
            return;
        }
        super.getCallbacksAndTopics()
             .forEach((cb, topicPartitions) -> {
                 topicPartitions.forEach(tp -> {
                     if (seekTopics.contains(tp.topic())) {
                         cb.seekToTimestamp(tp.topic(), tp.partition(), time);
                     }
                 });
             });
    }
}
