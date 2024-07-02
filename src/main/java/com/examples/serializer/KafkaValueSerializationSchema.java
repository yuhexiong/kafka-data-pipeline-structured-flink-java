package com.examples.serializer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.nio.charset.StandardCharsets;

public class KafkaValueSerializationSchema<T> implements KafkaRecordSerializationSchema<T> {
    final private String topic;

    public KafkaValueSerializationSchema(String topic) {
        this.topic = topic;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(T value, KafkaSinkContext kafkaSinkContext, Long aLong) {
        try {
            return new ProducerRecord<>(this.topic, new ObjectMapper().writeValueAsString(value).getBytes(StandardCharsets.UTF_8));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}