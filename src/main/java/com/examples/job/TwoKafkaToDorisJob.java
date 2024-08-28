package com.examples.job;

import com.examples.deserializer.KafkaValueDeserializationSchema;
import com.examples.entity.ProductEvent;
import com.examples.entity.SaleEvent;
import com.examples.function.SaleReportToRowDataFunction;
import com.examples.parser.YamlParser;
import com.examples.parser.sink.DorisSinkConfig;
import com.examples.parser.source.KafkaSourceConfig;
import com.examples.function.SaleReportBroadcastProcessFunction;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.types.DataType;

public class TwoKafkaToDorisJob extends AbstractFlinkPipelineJob {
    public TwoKafkaToDorisJob(String[] args) {
        super(args);
    }

    public static void main(String[] args) throws Exception {
        TwoKafkaToDorisJob job = new TwoKafkaToDorisJob(args);
        YamlParser yamlParser = job.getYamlParser();


        // kafka source ProductEvent
        KafkaSourceConfig productSourceConfig = yamlParser.getSourceConfig(KafkaSourceConfig.class, "kafka-product");
        KafkaSource<ProductEvent> productSource = productSourceConfig.buildSource(new KafkaValueDeserializationSchema<>(ProductEvent.class));
        DataStream<ProductEvent> productStream = job.env.fromSource(productSource, WatermarkStrategy.noWatermarks(), productSourceConfig.getName());

        // kafka source SaleEvent
        KafkaSourceConfig saleSourceConfig = yamlParser.getSourceConfig(KafkaSourceConfig.class, "kafka-sale");
        KafkaSource<SaleEvent> saleSource = saleSourceConfig.buildSource(new KafkaValueDeserializationSchema<>(SaleEvent.class));
        DataStream<SaleEvent> saleStream = job.env.fromSource(saleSource, WatermarkStrategy.noWatermarks(), saleSourceConfig.getName());

        // doris sink
        DorisSinkConfig sinkConfig = yamlParser.getSinkConfig(DorisSinkConfig.class, "doris");
        DorisSink<GenericRowData> dorisSink = sinkConfig.buildSink(
                new String[]{
                        "sale_id", "product_id", "unit", "unit_price", "total_price", "sale_date", "product_name", "product_unit_cost", "profit"
                },
                new DataType[]{
                        DataTypes.VARCHAR(50), DataTypes.VARCHAR(50), DataTypes.INT(), DataTypes.DOUBLE(),
                        DataTypes.DOUBLE(), DataTypes.TIMESTAMP(9), DataTypes.VARCHAR(100), DataTypes.DOUBLE(), DataTypes.DOUBLE()
                }
        );

        // connect
        SaleReportBroadcastProcessFunction broadcastProcessFunction = new SaleReportBroadcastProcessFunction();
        BroadcastStream<ProductEvent> productBroadcastStream = productStream.broadcast(broadcastProcessFunction.getMapStateDescriptor());
        saleStream.connect(productBroadcastStream).process(broadcastProcessFunction).map(new SaleReportToRowDataFunction()).sinkTo(dorisSink);

        job.run();
    }
}
