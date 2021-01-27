package com.wb.common.risk;

/**
 * 付款信息
 */
public class Pay {
    /**
     * 付款人
     */
    private String fromUid;
    /**
     * 收款人
     */
    private String toUid;
    /**
     * 交易金额
     */
    private Integer amount;

    /**
     * 规则id
     */
    private Integer ruleId;

    private Long eventTime;

    public Pay() {
    }

    public Pay(String fromUid, String toUid, Integer amount, Integer ruleId) {
        this.fromUid = fromUid;
        this.toUid = toUid;
        this.amount = amount;
        this.ruleId = ruleId;
        this.eventTime = System.currentTimeMillis();
    }

    public String getFromUid() {
        return fromUid;
    }

    public void setFromUid(String fromUid) {
        this.fromUid = fromUid;
    }

    public String getToUid() {
        return toUid;
    }

    public void setToUid(String toUid) {
        this.toUid = toUid;
    }

    public Integer getAmount() {
        return amount;
    }

    public void setAmount(Integer amount) {
        this.amount = amount;
    }

    public Integer getRuleId() {
        return ruleId;
    }

    public void setRuleId(Integer ruleId) {
        this.ruleId = ruleId;
    }

    public Long getEventTime() {
        return eventTime;
    }

    public void setEventTime(Long eventTime) {
        this.eventTime = eventTime;
    }
}
