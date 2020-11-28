package com.wb.common;

// 交易事件
public class ReceiptEvents {
    // 订单id
    private String orId;
    // 支付渠道
    private String payChannel;
    // 时间 单位 s
    private Long timestamp;

    public String getOrId() {
        return orId;
    }

    public void setOrId(String orId) {
        this.orId = orId;
    }

    public String getPayChannel() {
        return payChannel;
    }

    public void setPayChannel(String payChannel) {
        this.payChannel = payChannel;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public ReceiptEvents() {
    }

    public ReceiptEvents(String orId, String payChannel, Long timestamp) {
        this.orId = orId;
        this.payChannel = payChannel;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "ReceiptEvents{" +
                "orId='" + orId + '\'' +
                ", payEquipment='" + payChannel + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
