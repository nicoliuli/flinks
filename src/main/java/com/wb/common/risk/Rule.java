package com.wb.common.risk;

public class Rule {
    private Integer ruleId;
    // 分组key
    private String groupKeyName;
    // 阀值
    private Integer limit;
    // 窗口长度，单位s
    private Long window;
    // 聚合函数
    private String aggregateFunctionType;

    public Rule() {
    }

    public Rule(Integer ruleId, String groupKeyName, Integer limit, Long window, String aggregateFunctionType) {
        this.ruleId = ruleId;
        this.groupKeyName = groupKeyName;
        this.limit = limit;
        this.window = window;
        this.aggregateFunctionType = aggregateFunctionType;
    }

    public Integer getRuleId() {
        return ruleId;
    }

    public void setRuleId(Integer ruleId) {
        this.ruleId = ruleId;
    }

    public String getGroupKeyName() {
        return groupKeyName;
    }

    public void setGroupKeyName(String groupKeyName) {
        this.groupKeyName = groupKeyName;
    }

    public Integer getLimit() {
        return limit;
    }

    public void setLimit(Integer limit) {
        this.limit = limit;
    }

    public Long getWindow() {
        return window;
    }

    public void setWindow(Long window) {
        this.window = window;
    }

    public String getAggregateFunctionType() {
        return aggregateFunctionType;
    }

    public void setAggregateFunctionType(String aggregateFunctionType) {
        this.aggregateFunctionType = aggregateFunctionType;
    }
}
