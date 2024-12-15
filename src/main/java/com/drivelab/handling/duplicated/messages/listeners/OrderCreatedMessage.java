package com.drivelab.handling.duplicated.messages.listeners;

import lombok.NoArgsConstructor;

@NoArgsConstructor
public class OrderCreatedMessage {
    private String dishName;
    private Double price;

    public String getDishName() {
        return dishName;
    }

    public void setDishName(String dishName) {
        this.dishName = dishName;
    }

    public Double getPrice() {
        return price;
    }

    public void setPrice(Double price) {
        this.price = price;
    }
}
