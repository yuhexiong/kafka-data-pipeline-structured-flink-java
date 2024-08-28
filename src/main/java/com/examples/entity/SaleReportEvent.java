package com.examples.entity;

import lombok.Data;

@Data
public class SaleReportEvent {
    private String saleId;
    private String productId;
    private int unit;
    private double unitPrice;
    private double totalPrice;
    private Long saleDate;
    private String productName;
    private double productUnitCost;
    private double profit;
}