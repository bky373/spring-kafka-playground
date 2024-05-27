//package com.bky373.springkafkaplayground;
//
//import org.junit.jupiter.api.Test;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.boot.test.context.SpringBootTest;
//import org.springframework.kafka.core.KafkaTemplate;
//import org.springframework.kafka.test.context.EmbeddedKafka;
//import org.springframework.test.annotation.DirtiesContext;
//
//@SpringBootTest
//@EmbeddedKafka(bootstrapServersProperty = "spring.kafka.bootstrap-servers")
//@DirtiesContext
//class KafkaGh2302ApplicationTest {
//
//    @Autowired
//    KafkaGh2302Application.Listener listener;
//
//    @Autowired
//    KafkaTemplate<String, String> template;
//
//    @Test
//    void contextLoads() throws InterruptedException {
//
//        for (int i = 0; i < 50; i++) {
//            this.template.send("seekExample", i % 3, "some_key", "test#" + i);
//        }
//
//        Thread.sleep(1000);
//        System.out.println("====================================");
//        this.listener.seekToStart();
//        Thread.sleep(10000);
//    }
//
//}
