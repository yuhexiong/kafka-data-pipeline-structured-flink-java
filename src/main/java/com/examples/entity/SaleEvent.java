package com.examples.entity;

import lombok.Data;

@Data
public class SaleEvent {
    private String id;
    private String productId;
    private int unit;
    private double unitPrice;
    private double totalPrice;
    private String saleDate;
}