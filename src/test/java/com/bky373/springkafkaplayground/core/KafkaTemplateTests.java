package com.bky373.springkafkaplayground.core;

import static org.assertj.core.api.Assertions.allOf;
import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.kafka.test.assertj.KafkaConditions.keyValue;
import static org.springframework.kafka.test.assertj.KafkaConditions.partition;
import static org.springframework.kafka.test.assertj.KafkaConditions.value;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.core.ProducerFactory.Listener;
import org.springframework.kafka.core.ProducerPostProcessor;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.TopicPartitionOffset;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.condition.EmbeddedKafkaCondition;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.messaging.support.MessageBuilder;

@EmbeddedKafka(topics = KafkaTemplateTests.INT_KEY_TOPIC)
public class KafkaTemplateTests {

    public static final String INT_KEY_TOPIC = "intKeyTopic";
    private static EmbeddedKafkaBroker embeddedKafka;
    private static Consumer<Integer, String> consumer;
    private static final ProducerFactory.Listener<String, String> noopListener = new Listener<String, String>() {
        @Override
        public void producerAdded(String id, Producer<String, String> producer) {
        }

        @Override
        public void producerRemoved(String id, Producer<String, String> producer) {
        }
    };

    private static final ProducerPostProcessor<String, String> noopProducerPostProcessor = producer -> producer;

    @BeforeAll
    public static void setUp() {
        embeddedKafka = EmbeddedKafkaCondition.getBroker();
        var consumerProps = KafkaTestUtils.consumerProps("testGroup", "false", embeddedKafka);
        var cf = new DefaultKafkaConsumerFactory<Integer, String>(consumerProps);
        consumer = cf.createConsumer();
        embeddedKafka.consumeFromAnEmbeddedTopic(consumer, INT_KEY_TOPIC);
    }

    @AfterAll
    public static void tearDown() {
        consumer.close();
    }

    @Test
    void testTemplate() {
        var producerProps = KafkaTestUtils.producerProps(embeddedKafka);
        var pf = new DefaultKafkaProducerFactory<Integer, String>(producerProps);
        AtomicReference<Producer<Integer, String>> wrapped = new AtomicReference<>();
        pf.addPostProcessor(pd -> {
            var pxFactory = new ProxyFactory();
            pxFactory.setTarget(pd);
            var proxy = (Producer<Integer, String>) pxFactory.getProxy();
            wrapped.set(proxy);
            return proxy;
        });
        KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf, true);

        template.setDefaultTopic(INT_KEY_TOPIC);

        template.setConsumerFactory(
                new DefaultKafkaConsumerFactory<>(KafkaTestUtils.consumerProps("xx", "false", embeddedKafka)));
        ConsumerRecords<Integer, String> initialRecords =
                template.receive(Collections.singleton(new TopicPartitionOffset(INT_KEY_TOPIC, 1, 1L)));
        assertThat(initialRecords).isEmpty();

        template.sendDefault("foo");
        assertThat(KafkaTestUtils.getSingleRecord(consumer, INT_KEY_TOPIC)).has(value("foo"));

        template.sendDefault(0, 2, "bar");
        ConsumerRecord<Integer, String> received = KafkaTestUtils.getSingleRecord(consumer, INT_KEY_TOPIC);
        assertThat(received).has(allOf(keyValue(2, "bar"), partition(0)));

        template.send(INT_KEY_TOPIC, 0, 2, "baz");
        received = KafkaTestUtils.getSingleRecord(consumer, INT_KEY_TOPIC);
        assertThat(received).has(allOf(keyValue(2, "baz"), partition(0)));

        template.send(INT_KEY_TOPIC, 1, null, "qux");
        received = KafkaTestUtils.getSingleRecord(consumer, INT_KEY_TOPIC);
        assertThat(received).has(allOf(keyValue(null, "qux"), partition(1)));

        template.send(MessageBuilder.withPayload("fiz")
                                    .setHeader(KafkaHeaders.TOPIC, INT_KEY_TOPIC)
                                    .setHeader(KafkaHeaders.PARTITION, 0)
                                    .setHeader(KafkaHeaders.KEY, 2)
                                    .build());
        received = KafkaTestUtils.getSingleRecord(consumer, INT_KEY_TOPIC);
        assertThat(received).has(allOf(keyValue(2, "fiz"), partition(0)));

        template.send(MessageBuilder.withPayload("buz")
                                    .setHeader(KafkaHeaders.PARTITION, 1)
                                    .setHeader(KafkaHeaders.KEY, 2)
                                    .build());
        received = KafkaTestUtils.getSingleRecord(consumer, INT_KEY_TOPIC);
        assertThat(received).has(allOf(keyValue(2, "buz"), partition(1)));

        Map<MetricName, ? extends Metric> metrics = template.execute(Producer::metrics);
        assertThat(metrics).isNotNull();
        metrics = template.metrics();
        assertThat(metrics).isNotNull();
        var partitions = template.partitionsFor(INT_KEY_TOPIC);
        assertThat(partitions).isNotNull();
        assertThat(partitions).hasSize(2);
        var expected = wrapped.get();
        var delegate = KafkaTestUtils.getPropertyValue(pf.createProducer(), "delegate");
        assertThat(delegate).isSameAs(expected);

        var currentReceived = template.receive(INT_KEY_TOPIC, 1, received.offset());
        assertThat(currentReceived).has(allOf(keyValue(2, "buz"), partition(1)))
                                   .extracting(ConsumerRecord::offset)
                                   .isEqualTo(received.offset());
        var receivedList = template.receive(List.of(
                new TopicPartitionOffset(INT_KEY_TOPIC, 1, 1L),
                new TopicPartitionOffset(INT_KEY_TOPIC, 0, 1L),
                new TopicPartitionOffset(INT_KEY_TOPIC, 0, 0L),
                new TopicPartitionOffset(INT_KEY_TOPIC, 1, 0L)));
        assertThat(receivedList.count()).isEqualTo(4);
        var partitions2 = receivedList.partitions();
        assertThat(partitions2).containsExactly(
                new TopicPartition(INT_KEY_TOPIC, 1),
                new TopicPartition(INT_KEY_TOPIC, 0));
        assertThat(receivedList.records(new TopicPartition(INT_KEY_TOPIC, 1)))
                .extracting(ConsumerRecord::offset)
                .containsExactly(1L, 0L);
        assertThat(receivedList.records(new TopicPartition(INT_KEY_TOPIC, 0)))
                .extracting(ConsumerRecord::offset)
                .containsExactly(1L, 0L);
        pf.destroy();
    }
}
