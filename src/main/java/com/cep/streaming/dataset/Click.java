package com.cep.streaming.dataset;

/**
 * Created by jayant on 7/28/15.
 */
public class Click {
    private long customerId;
    private String city;
    private long productId;
    private String ip;

    public Click() {

    }
    public Click(String str) {
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

    public long getProductId() {
        return productId;
    }

    public void setProductId(long productId) {
        this.productId = productId;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

}


