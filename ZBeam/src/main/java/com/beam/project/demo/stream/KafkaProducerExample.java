package com.beam.project.demo.stream;

import com.beam.project.demo.window.SchoolMessageProducer;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class KafkaProducerExample {

    public static void main(String[] args) throws Exception {

        SchoolMessageProducer producer = new SchoolMessageProducer("data-master01:9092,data-worker01:9092,data-worker02:9092", "SchoolClassTopic");
        producer.setTaskSend(true);
        producer.doSendMessage();
    }
}
