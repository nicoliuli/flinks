package com.wb.common.risk;

public class Wrapper {
    private String key;
    private Pay pay;

    public Wrapper(String key, Pay pay) {
        this.key = key;
        this.pay = pay;
    }

    public Wrapper() {
    }

    public Pay getPay() {
        return pay;
    }

    public void setPay(Pay pay) {
        this.pay = pay;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }
}
