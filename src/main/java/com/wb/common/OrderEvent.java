package com.wb.common;

public class OrderEvent {
    private Long orderId;
    private String type;
    // 时间，秒
    private Long time;

    public OrderEvent() {
    }

    public OrderEvent(Long orderId, String type, Long time) {
        this.orderId = orderId;
        this.type = type;
        this.time = time;
    }

    public Long getOrderId() {
        return orderId;
    }

    public void setOrderId(Long orderId) {
        this.orderId = orderId;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Long getTime() {
        return time;
    }

    public void setTime(Long time) {
        this.time = time;
    }

    @Override
    public String toString() {
        return "OrderEvent{" +
                "orderId=" + orderId +
                ", type='" + type + '\'' +
                ", time=" + time +
                '}';
    }
}
