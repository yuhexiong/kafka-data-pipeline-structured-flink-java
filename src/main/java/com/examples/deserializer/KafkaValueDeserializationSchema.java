package com.examples.deserializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.nio.charset.StandardCharsets;

public class KafkaValueDeserializationSchema<T> implements KafkaRecordDeserializationSchema<T> {
    final Class<T> clazz;

    public KafkaValueDeserializationSchema(Class<T> clazz) {
        this.clazz = clazz;
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> consumerRecord, Collector<T> collector) {
        try {
            T data = new ObjectMapper().readValue(consumerRecord.value(), this.clazz);
            collector.collect(data);
        } catch (Exception e) {
            System.err.println("Failed to deserialize Kafka record into " + clazz.getSimpleName() + ": " + new String(consumerRecord.value(), StandardCharsets.UTF_8));
        }

    }

    @Override
    public TypeInformation<T> getProducedType() {
        return TypeInformation.of(this.clazz);
    }
}