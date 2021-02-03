package com.wb.common.risk;

public class Alert {
    /**
     * 聚合类型
     */
    private String aggregateFunctionType;
    /**
     * 分控阈值
     */
    private Integer limit;
    /**
     * 实际值
     */
    private Integer value;
    /**
     * 分组
     */
    private String groupKeyName;

    public Alert() {
    }

    public Alert(String aggregateFunctionType, Integer limit, Integer value, String groupKeyName) {
        this.aggregateFunctionType = aggregateFunctionType;
        this.limit = limit;
        this.value = value;
        this.groupKeyName = groupKeyName;
    }

    public String getAggregateFunctionType() {
        return aggregateFunctionType;
    }

    public void setAggregateFunctionType(String aggregateFunctionType) {
        this.aggregateFunctionType = aggregateFunctionType;
    }

    public Integer getLimit() {
        return limit;
    }

    public void setLimit(Integer limit) {
        this.limit = limit;
    }

    public Integer getValue() {
        return value;
    }

    public void setValue(Integer value) {
        this.value = value;
    }

    public String getGroupKeyName() {
        return groupKeyName;
    }

    public void setGroupKeyName(String groupKeyName) {
        this.groupKeyName = groupKeyName;
    }

    @Override
    public String toString() {
        return "Alert{" +
                "aggregateFunctionType='" + aggregateFunctionType + '\'' +
                ", limit=" + limit +
                ", value=" + value +
                ", groupKeyName='" + groupKeyName + '\'' +
                '}';
    }
}
