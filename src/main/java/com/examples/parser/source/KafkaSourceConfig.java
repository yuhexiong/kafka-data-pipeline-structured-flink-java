package com.examples.parser.source;

import com.examples.parser.AbstractConfigParser;
import lombok.Getter;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;

import java.util.Map;

@Getter
public class KafkaSourceConfig extends AbstractConfigParser {
    String bootstrapServers;
    String topics;
    String groupId;
    String name;
    OffsetsInitializer offset;

    public KafkaSourceConfig(Map<String, Object> map){
        super(map);
    }

    @Override
    protected void initConfig(Map<String, Object> map) {
        this.bootstrapServers = String.valueOf(map.get("bootstrapServers"));
        this.topics = String.valueOf(map.get("topics"));
        this.groupId = String.valueOf(map.get("groupId"));
        this.name = String.valueOf(map.get("name"));

        // default earliest
        if (String.valueOf(map.get("offsetInitializationType")).equals("LATEST")) {
            offset = OffsetsInitializer.latest();
        } else if (String.valueOf(map.get("offsetInitializationType")).equals("COMMITTED")) {
            offset = OffsetsInitializer.committedOffsets();
        } else {
            offset = OffsetsInitializer.earliest();
        }
    }

    public <T> KafkaSource<T> buildSource(KafkaRecordDeserializationSchema<T> recordDeserializer) {
        return KafkaSource.<T>builder()
                .setBootstrapServers(this.bootstrapServers)
                .setTopics(this.topics)
                .setGroupId(this.groupId)
                .setStartingOffsets(this.offset)
                .setDeserializer(recordDeserializer)
                .build();
    }

}
