package com.wb.common;

// 订单事件
public class OrderEvents {
    // 用户id
    private Long userId;
    // 动作
    private String action;
    // 订单id
    private String orId;
    // 时间 单位 s
    private Long timestamp;

    public OrderEvents() {
    }

    public OrderEvents(Long userId, String action, String orId, Long timestamp) {
        this.userId = userId;
        this.action = action;
        this.orId = orId;
        this.timestamp = timestamp;
    }

    public Long getUserId() {
        return userId;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
    }

    public String getAction() {
        return action;
    }

    public void setAction(String action) {
        this.action = action;
    }

    public String getOrId() {
        return orId;
    }

    public void setOrId(String orId) {
        this.orId = orId;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "OrderEvents{" +
                "userId=" + userId +
                ", action='" + action + '\'' +
                ", orId='" + orId + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
