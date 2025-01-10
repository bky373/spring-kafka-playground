package com.bky373.springkafkaplayground;

import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.listener.AbstractConsumerSeekAware;

import java.util.Map;

public class BaseListener extends AbstractConsumerSeekAware {

    @Override
    public void onPartitionsAssigned(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
        assignments.forEach((tp, o) -> callback.seekToEnd(tp.topic(), tp.partition()));
    }
}
