package com.bky373.springkafkaplayground;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.IntStream;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConsumerSeekAware;
import org.springframework.stereotype.Component;

@SpringBootApplication
public class SeekExampleApplication {

    public static void main(String[] args) {
        SpringApplication.run(SeekExampleApplication.class, args);
    }

    @Bean
    public ApplicationRunner runner(Listener listener, KafkaTemplate<String, String> template) {
        return args -> {
            IntStream.range(0, 10)
                     .forEach(i -> template.send(
                             new ProducerRecord<>("seekRelative-negative-true", i % 3, "test-k-" + i, "test-v-" + i)
                     ));
            while (true) {
                System.in.read();
                listener.seekToStart();
            }
        };
    }

    @Bean
    public NewTopic topic() {
        return new NewTopic("seekRelative-negative-true", 3, (short) 1);
    }

    @Component
    class Listener implements ConsumerSeekAware {

        private static final Logger logger = LoggerFactory.getLogger(Listener.class);
        private final ThreadLocal<ConsumerSeekCallback> callbackForThread = new ThreadLocal<>();
        private final Map<TopicPartition, ConsumerSeekCallback> callbacks = new ConcurrentHashMap<>();

        @Override
        public void registerSeekCallback(ConsumerSeekCallback callback) {
            this.callbackForThread.set(callback);
        }

        @Override
        public void onPartitionsAssigned(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
            assignments.keySet()
                       .forEach(tp -> this.callbacks.put(tp, this.callbackForThread.get()));
        }

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            partitions.forEach(this.callbacks::remove);
            this.callbackForThread.remove();
        }

        @Override
        public void onIdleContainer(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
        }

        @KafkaListener(id = "seekRelative-negative-true", topics = "seekRelative-negative-true", concurrency = "3")
        public void listen(ConsumerRecord<String, String> in) {
            logger.info("########## in = {}", in.toString());
        }

        public void seekToStart() {
            this.callbacks.forEach((tp, callback) -> callback.seekRelative(tp.topic(), tp.partition(), -5, true));
        }
    }
}
