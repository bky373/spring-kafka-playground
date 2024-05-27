package com.bky373.springkafkaplayground.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.listener.AbstractConsumerSeekAware;
import org.springframework.kafka.listener.ConsumerSeekAware.ConsumerSeekCallback;
import org.springframework.kafka.test.utils.KafkaTestUtils;

public class ConsumerSeekAwareTests {

    @Test
    void beginningEndAndBulkSeekToTimestamp() throws ExecutionException, InterruptedException {
        class CSA extends AbstractConsumerSeekAware {

        }

        AbstractConsumerSeekAware csa = new CSA();
        var exec1 = Executors.newSingleThreadExecutor();
        var exec2 = Executors.newSingleThreadExecutor();
        var cb1 = mock(ConsumerSeekCallback.class);
        var cb2 = mock(ConsumerSeekCallback.class);
        var ab = new AtomicBoolean(true);
        var map1 = new LinkedHashMap<>(Map.of(new TopicPartition("foo", 0), 0L, new TopicPartition("foo", 1), 0L));
        var map2 = new LinkedHashMap<>(Map.of(new TopicPartition("foo", 2), 0L, new TopicPartition("foo", 3), 0L));
        var register = (Callable<Void>) () -> {
            if (ab.getAndSet(false)) {
                System.out.println("map1 = " + map1);
                csa.registerSeekCallback(cb1);
                csa.onPartitionsAssigned(map1, null);
            } else {
                System.out.println("map2 = " + map2);
                csa.registerSeekCallback(cb2);
                csa.onPartitionsAssigned(map2, null);
            }
            return null;
        };
        exec1.submit(register)
             .get();
        exec2.submit(register)
             .get();
        csa.seekToBeginning();
        verify(cb1).seekToBeginning(new LinkedList<>(map1.keySet()));
        verify(cb2).seekToBeginning(new LinkedList<>(map2.keySet()));
        csa.seekToEnd();
        verify(cb1).seekToEnd(new LinkedList<>(map1.keySet()));
        verify(cb2).seekToEnd(new LinkedList<>(map2.keySet()));
        csa.seekToTimestamp(42L);
        verify(cb1).seekToTimestamp(new LinkedList<>(map1.keySet()), 42L);
        verify(cb2).seekToTimestamp(new LinkedList<>(map2.keySet()), 42L);
        var revoke1 = (Callable<Void>) () -> {
            if (!ab.getAndSet(true)) {
                csa.onPartitionsRevoked(Collections.singleton(map1.keySet()
                                                                  .iterator()
                                                                  .next()));
            } else {
                csa.onPartitionsRevoked(Collections.singleton(map2.keySet()
                                                                  .iterator()
                                                                  .next()));
            }
            return null;
        };
        exec1.submit(revoke1)
             .get();
        exec2.submit(revoke1)
             .get();
        map1.remove(map1.keySet()
                        .iterator()
                        .next());
        map2.remove(map2.keySet()
                        .iterator()
                        .next());
        csa.seekToTimestamp(43L);
        verify(cb1).seekToTimestamp(new LinkedList<>(map1.keySet()), 43L);
        verify(cb2).seekToTimestamp(new LinkedList<>(map2.keySet()), 43L);
        var revoke2 = (Callable<Void>) () -> {
            if (ab.getAndSet(false)) {
                csa.onPartitionsRevoked(Collections.singletonList(map1.keySet()
                                                                      .iterator()
                                                                      .next()));
            } else {
                csa.onPartitionsRevoked(Collections.singletonList(map2.keySet()
                                                                      .iterator()
                                                                      .next()));
            }
            return null;
        };
        exec1.submit(revoke2)
             .get();
        exec2.submit(revoke2)
             .get();
        assertThat(KafkaTestUtils.getPropertyValue(csa, "callbacks", Map.class)).isEmpty();
        assertThat(KafkaTestUtils.getPropertyValue(csa, "callbacksToTopic", Map.class)).isEmpty();
        var checkTL = (Callable<Void>) () -> {
            csa.unregisterSeekCallback();
            assertThat(KafkaTestUtils.getPropertyValue(csa, "callbackForThread", Map.class)
                                     .get(Thread.currentThread())).isNull();
            return null;
        };
        exec1.submit(checkTL)
             .get();
        exec2.submit(checkTL)
             .get();
        exec1.shutdown();
        exec2.shutdown();
    }
}