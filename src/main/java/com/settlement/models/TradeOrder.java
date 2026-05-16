package com.settlement.models;

public class TradeOrder {
    private String orderId;
    private String asset;

    public TradeOrder(){};

    public TradeOrder(String orderId,String asset){
        this.asset=asset;
        this.orderId=orderId;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getAsset() {
        return asset;
    }

    public void setAsset(String asset) {
        this.asset = asset;
    }


    @Override
    public String toString() {
        return "TradeOrder{orderId='" + orderId + "', asset='" + asset + "'}";
    }
}
