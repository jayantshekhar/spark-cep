package com.cep.streaming.dataset;

/**
 * Created by jayant on 7/28/15.
 */
public class Transaction {
    private long customerId;
    private String city;
    private double amount;
    private String ip;

    public Transaction() {

    }
    public Transaction(String str) {
        String[] parts = str.split(",");
        setCustomerId(Long.parseLong(parts[0]));
        setCity(parts[1]);
        setIp(parts[2]);
    }

    public long getCustomerId() {
        return customerId;
    }

    public void setCustomerId(long customerId) {
        this.customerId = customerId;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public double getAmount() {
        return amount;
    }

    public void setAmount(double amount) {
        this.amount = amount;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

}


