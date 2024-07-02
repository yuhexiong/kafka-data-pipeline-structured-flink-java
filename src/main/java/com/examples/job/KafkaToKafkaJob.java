package com.examples.job;

import com.examples.deserializer.KafkaValueDeserializationSchema;
import com.examples.entity.ProductEvent;
import com.examples.parser.YamlParser;
import com.examples.parser.sink.KafkaSinkConfig;
import com.examples.parser.source.KafkaSourceConfig;
import com.examples.serializer.KafkaValueSerializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;

public class KafkaToKafkaJob extends AbstractFlinkPipelineJob {
    public KafkaToKafkaJob(String[] args) {
        super(args);
    }

    public static void main(String[] args) throws Exception {
        KafkaToKafkaJob job = new KafkaToKafkaJob(args);

        YamlParser yamlParser = job.getYamlParser();
        KafkaSourceConfig sourceConfig = yamlParser.getSourceConfig(KafkaSourceConfig.class, "kafka");
        KafkaSource<ProductEvent> source = sourceConfig.buildSource(new KafkaValueDeserializationSchema<>(ProductEvent.class));
        DataStream<ProductEvent> stream = job.env.fromSource(source, WatermarkStrategy.noWatermarks(), sourceConfig.getName());

        KafkaSinkConfig sinkConfig = yamlParser.getSinkConfig(KafkaSinkConfig.class, "kafka");
        KafkaSink<ProductEvent> sink = sinkConfig.buildSink(new KafkaValueSerializationSchema<>(sinkConfig.getTopics()));

        stream.sinkTo(sink);
        stream.print();

        job.run();
    }
}
