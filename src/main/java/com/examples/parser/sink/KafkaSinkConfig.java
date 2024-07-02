package com.examples.parser.sink;

import com.examples.parser.AbstractConfigParser;
import lombok.Getter;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;

import java.util.Map;

@Getter
public class KafkaSinkConfig extends AbstractConfigParser {
    String bootstrapServers;
    String topics;

    public KafkaSinkConfig(Map<String, Object> map){
        super(map);
    }

    @Override
    protected void initConfig(Map<String, Object> map) {
        this.bootstrapServers = String.valueOf(map.get("bootstrapServers"));
        this.topics = String.valueOf(map.get("topics"));
    }

    public <T> KafkaSink<T> buildSink(KafkaRecordSerializationSchema<T> recordSerializer) {
        return KafkaSink.<T>builder()
                .setBootstrapServers(this.bootstrapServers)
                .setRecordSerializer(recordSerializer)
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE) // guarantee at least send one time
                .build();
    }

}
