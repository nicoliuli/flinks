package com.wb.common;

public class TestModel {
    private long id;
    private String testabc;

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getTestabc() {
        return testabc;
    }

    public void setTestabc(String testabc) {
        this.testabc = testabc;
    }

    public TestModel() {
    }

    public TestModel(long id, String testabc) {
        this.id = id;
        this.testabc = testabc;
    }
}
