package com.beam.project.demo.stream;

import com.beam.project.common.SnowFlakeUtil;
import com.beam.project.demo.bean.SchoolClass;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

@Slf4j
public class KafkaProducerExample {

    public static void main(String[] args) throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "data-master01:9092,data-worker01:9092,data-worker02:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        // 其他配置...
        Producer<Long, String> producer = new KafkaProducer<>(props);

        long startTime = System.nanoTime();
        for (long i = 0; i < 100; i++) {
            Long dataKey = SnowFlakeUtil.getSnowFlakeId();
            //Long dataKey = i;
            SchoolClass schoolClass = new SchoolClass();
            schoolClass.setSchoolCode("S" + (i % 4));
            schoolClass.setClassCode("C" + dataKey);
            //schoolClass.setSchoolCode("S" + dataKey);
            //schoolClass.setClassCode("C" + (i % 4));
            producer.send(new ProducerRecord<Long, String>("SchoolClassTopic", dataKey, objectMapper.writeValueAsString(schoolClass)),
                    (metadata, exception) -> {
                        if (exception != null) {
                            log.error("SchoolClass Send Fail: {}", schoolClass);
                        }
                    });
        }
        producer.flush(); // 确保所有消息都发送完毕
        long endTime = System.nanoTime();
        System.out.println("Messages sent in " + TimeUnit.NANOSECONDS.toMillis(endTime - startTime) + " ms");
        producer.close();
    }
}
