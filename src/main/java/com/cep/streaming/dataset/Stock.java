package com.cep.streaming.dataset;

import java.io.Serializable;

/**
 * Created by jayant on 7/19/15.
 */
public class Stock implements Serializable {

    private String symbol;
    private double price;
    private double ask;
    private double bid;

    public Stock() {

    }

    public Stock(String str) {
        String[] parts = str.split(",");
        setSymbol(parts[0]);
        setPrice(Double.parseDouble(parts[1].trim()));

    }

    public String getSymbol() {
        return symbol;
    }

    public void setSymbol(String symbol) {
        this.symbol = symbol;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    public double getAsk() {
        return ask;
    }

    public void setAsk(double ask) {
        this.price = price;
    }

    public double getBid() {
        return bid;
    }

    public void setBid(double bid) {
        this.bid = bid;
    }

}
