package com.bky373.springkafkaplayground.auto_commit;

import static com.bky373.springkafkaplayground.auto_commit.KafkaAutoCommitConfig.AUTO_COMMIT_TOPIC;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class MyAutoCommitListener {

    @KafkaListener(
            id = "auto-commit",
            topics = AUTO_COMMIT_TOPIC,
            groupId = "auto-commit-group"
    )
    public void listen(ConsumerRecord record) throws InterruptedException {
//        log.info("[Begin] Received: {}", input);
        Thread.sleep(2000);
        log.info("[End] Received: {}, offset: {}", record.value(), record.offset());
    }
}
