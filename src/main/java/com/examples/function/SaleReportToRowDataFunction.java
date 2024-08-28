package com.examples.function;

import com.examples.entity.SaleReportEvent;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;

public class SaleReportToRowDataFunction implements MapFunction<SaleReportEvent, GenericRowData> {

    @Override
    public GenericRowData map(SaleReportEvent data) throws Exception {
        GenericRowData rowData = new GenericRowData(9);
        rowData.setField(0, StringData.fromString(data.getSaleId()));
        rowData.setField(1, StringData.fromString(data.getProductId()));
        rowData.setField(2, data.getUnit());
        rowData.setField(3, data.getUnitPrice());
        rowData.setField(4, data.getTotalPrice());
        rowData.setField(5, TimestampData.fromEpochMillis(data.getSaleDate()));
        rowData.setField(6, StringData.fromString(data.getProductName()));
        rowData.setField(7, data.getProductUnitCost());
        rowData.setField(8, data.getProfit());
        return rowData;
    }
}
