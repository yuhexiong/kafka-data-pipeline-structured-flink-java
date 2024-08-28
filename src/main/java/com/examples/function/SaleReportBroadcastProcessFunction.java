package com.examples.function;

import com.examples.entity.ProductEvent;
import com.examples.entity.SaleEvent;
import com.examples.entity.SaleReportEvent;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class SaleReportBroadcastProcessFunction extends BroadcastProcessFunction<SaleEvent, ProductEvent, SaleReportEvent> {
    MapStateDescriptor<String, ProductEvent> productDescriptor = new MapStateDescriptor<>("productMap", BasicTypeInfo.STRING_TYPE_INFO,
            TypeInformation.of(new TypeHint<>() {
            }));

    public MapStateDescriptor<String,ProductEvent> getMapStateDescriptor() {
        return productDescriptor;
    }

    @Override
    public void processElement(SaleEvent saleEvent, BroadcastProcessFunction<SaleEvent, ProductEvent, SaleReportEvent>.ReadOnlyContext readOnlyContext, Collector<SaleReportEvent> collector) throws Exception {
        ProductEvent productEvent = readOnlyContext.getBroadcastState(productDescriptor).get(saleEvent.getProductId());

        // product not found
        if (productEvent == null) {
            return;
        }

        SaleReportEvent saleReportEvent = getSaleReportEvent(saleEvent, productEvent);
        collector.collect(saleReportEvent);
    }

    private SaleReportEvent getSaleReportEvent(SaleEvent saleEvent, ProductEvent productEvent) {
        SaleReportEvent saleReportEvent = new SaleReportEvent();
        saleReportEvent.setSaleId(saleEvent.getId());
        saleReportEvent.setProductId(saleEvent.getProductId());

        int unit = saleEvent.getUnit();
        saleReportEvent.setUnit(unit);
        saleReportEvent.setUnitPrice(saleEvent.getUnitPrice());

        double totalPrice = saleEvent.getTotalPrice();
        saleReportEvent.setTotalPrice(totalPrice);

        String saleDate = saleEvent.getSaleDate();
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX");
        long timestamp = 0;
        try {
            Date date = dateFormat.parse(saleDate);
            timestamp = date.getTime();
            timestamp = timestamp > Math.pow(10, 11) * 1000 ? timestamp : timestamp * 1000;
        } catch (ParseException e) {
            e.printStackTrace();
        }

        saleReportEvent.setSaleDate(timestamp);

        saleReportEvent.setProductName(productEvent.getName());
        double unitCost = productEvent.getCost();
        saleReportEvent.setProductUnitCost(unitCost);
        saleReportEvent.setProfit(totalPrice - unitCost*unit);

        return saleReportEvent;
    }

    @Override
    public void processBroadcastElement(ProductEvent productEvent, BroadcastProcessFunction<SaleEvent, ProductEvent, SaleReportEvent>.Context context, Collector<SaleReportEvent> collector) throws Exception {
        context.getBroadcastState(productDescriptor).put(productEvent.getId(), productEvent);
    }
}
