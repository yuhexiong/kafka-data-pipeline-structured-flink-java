package com.examples.parser.sink;

import com.examples.parser.AbstractConfigParser;
import lombok.Getter;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.serializer.DorisRecordSerializer;
import org.apache.doris.flink.sink.writer.serializer.RowDataSerializer;
import org.apache.flink.table.types.DataType;

import java.util.Map;
import java.util.Properties;

@Getter
public class DorisSinkConfig extends AbstractConfigParser {
    String fenodes;
    String table;
    String username;
    String password;

    public DorisSinkConfig(Map<String, Object> map){
        super(map);
    }

    @Override
    protected void initConfig(Map<String, Object> map) {
        this.fenodes = String.valueOf(map.get("fenodes"));
        this.table = String.valueOf(map.get("table"));
        this.username = String.valueOf(map.get("username"));
        this.password = String.valueOf(map.get("password"));
    }

    public <T> DorisSink<T> buildSink(String[] fields, DataType[] types) {
        DorisSink.Builder<T> builder = DorisSink.builder();
        DorisOptions.Builder dorisBuilder = DorisOptions.builder()
                .setFenodes(this.fenodes)
                .setTableIdentifier(this.table)
                .setUsername(this.username)
                .setPassword(this.password);

        Properties properties = new Properties();
        properties.setProperty("format", "json");
        properties.setProperty("read_json_by_line", "true");

        DorisExecutionOptions.Builder executionBuilder = DorisExecutionOptions.builder();
        executionBuilder.setLabelPrefix("labelPrefix")
                .setDeletable(false)
                .setStreamLoadProp(properties); // set properties

        RowDataSerializer rowDataSerializer = RowDataSerializer.builder().setFieldNames(fields).setType("json").setFieldType(types).build();
        builder.setDorisReadOptions(DorisReadOptions.builder().build())
                .setDorisExecutionOptions(executionBuilder.build())
                .setSerializer((DorisRecordSerializer<T>) rowDataSerializer)
                .setDorisOptions(dorisBuilder.build());

        return builder.build();
    }

}
