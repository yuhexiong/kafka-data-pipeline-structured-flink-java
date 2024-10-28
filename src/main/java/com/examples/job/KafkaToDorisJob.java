package com.examples.job;

import com.examples.deserializer.KafkaValueDeserializationSchema;
import com.examples.entity.ProductEvent;
import com.examples.function.ProductEventDorisMapFunction;
import com.examples.parser.YamlParser;
import com.examples.parser.sink.DorisSinkConfig;
import com.examples.parser.source.KafkaSourceConfig;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.types.DataType;

public class KafkaToDorisJob extends AbstractFlinkPipelineJob {
    public KafkaToDorisJob(String[] args) {
        super(args);
    }

    public static void main(String[] args) throws Exception {
        KafkaToDorisJob job = new KafkaToDorisJob(args);
        YamlParser yamlParser = job.getYamlParser();

        // kafka source
        KafkaSourceConfig sourceConfig = yamlParser.getSourceConfig(KafkaSourceConfig.class, "kafka");
        KafkaSource<ProductEvent> source = sourceConfig.buildSource(new KafkaValueDeserializationSchema<>(ProductEvent.class));
        DataStream<ProductEvent> stream = job.env.fromSource(source, WatermarkStrategy.noWatermarks(), sourceConfig.getJobName());

        // doris sink
        DorisSinkConfig sinkConfig = yamlParser.getSinkConfig(DorisSinkConfig.class, "doris");
        DorisSink<GenericRowData> dorisSink = sinkConfig.buildSink(
            new String[]{"id", "name", "category", "manufacturer", "description", "cost"},
            new DataType[]{DataTypes.VARCHAR(10), DataTypes.VARCHAR(50), DataTypes.VARCHAR(50), DataTypes.VARCHAR(50), DataTypes.VARCHAR(50), DataTypes.DOUBLE()});

        // connect
        stream.map(new ProductEventDorisMapFunction()).sinkTo(dorisSink);
        stream.print();

        job.run();
    }
}
