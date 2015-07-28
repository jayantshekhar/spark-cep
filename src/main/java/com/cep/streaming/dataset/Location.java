package com.cep.streaming.dataset;

/**
 * Created by jayant on 7/28/15.
 */
public class Location {
    private long customerId;
    private String city;
    private String ip;

    public Location() {

    }

    public Location(String str) {
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

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }
}


