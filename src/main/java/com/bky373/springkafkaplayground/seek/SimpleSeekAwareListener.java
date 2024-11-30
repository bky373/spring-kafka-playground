package com.bky373.springkafkaplayground.seek;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.listener.AbstractConsumerSeekAware;
import org.springframework.kafka.retrytopic.TopicSuffixingStrategy;
import org.springframework.stereotype.Component;

@Component
public class SimpleSeekAwareListener extends AbstractConsumerSeekAware {

    @RetryableTopic(
            retryTopicSuffix = "-listener1", dltTopicSuffix = "-listener1-dlt",
            topicSuffixingStrategy = TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE)
    @KafkaListener(topics = "my-topic", groupId = "my-group-id")
    public void listen(String message) {
        System.out.println("Received: " + message);
    }

    @RetryableTopic(
            retryTopicSuffix = "-listener2", dltTopicSuffix = "-listener2-dlt",
            topicSuffixingStrategy = TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE)
    @KafkaListener(topics = "my-topic", groupId = "my-group-id2")
    public void listen2(String message2) {
        System.out.println("Received: " + message2);
    }

    // 오프셋을 맨 처음으로 이동하는 메서드
    public void seekToBeginning() {
        // 토픽 파티션 별로 콜백을 꺼내 사용
//        this.getTopicsAndCallbacks()
////            .forEach((topicPartition, callbacks) -> {
////                callbacks.forEach(cb -> cb.seekToBeginning(topicPartition.topic(), topicPartition.partition()));
////            });
    }
}

