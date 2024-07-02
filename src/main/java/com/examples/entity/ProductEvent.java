package com.examples.entity;

import lombok.Data;

@Data
public class ProductEvent {
    private String id;
    private String name;
    private String category;
    private String manufacturer;
    private String description;
    private double price;
}
