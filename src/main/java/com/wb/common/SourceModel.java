package com.wb.common;

public class SourceModel {
    private Long id;

    private long time;

    private String type;


    public SourceModel() {
    }

    public SourceModel(Long id, long time, String type) {
        this.id = id;
        this.time = time;
        this.type = type;
    }

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }
}
