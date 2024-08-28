package com.examples.function;

import com.examples.entity.ProductEvent;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;

public class ProductEventDorisMapFunction implements MapFunction<ProductEvent, GenericRowData> {

    /**
     * @param payloadData
     * @return
     * @throws Exception
     */
    @Override
    public GenericRowData map(ProductEvent payloadData) throws Exception {
        GenericRowData rowData = new GenericRowData(6);
        rowData.setField(0, StringData.fromString(payloadData.getId()));
        rowData.setField(1, StringData.fromString(payloadData.getName()));
        rowData.setField(2, StringData.fromString(payloadData.getCategory()));
        rowData.setField(3, StringData.fromString(payloadData.getManufacturer()));
        rowData.setField(4, StringData.fromString(payloadData.getDescription()));
        rowData.setField(5, payloadData.getCost());
        return rowData;
    }
}
