package com.wb.common;

import java.sql.Timestamp;

public class Sensor2 {
    /**
     * 设备编号
     */
    private String deviceId;


    private Integer temperature;

    /**
     * 时间戳
     */
    private Long timestamps;

    /**
     *  在sql中通过rowtime指定事件时间后，必须用Timestamp类型
     */
    private Timestamp eventTime;


    public String getDeviceId() {
        return deviceId;
    }

    public void setDeviceId(String deviceId) {
        this.deviceId = deviceId;
    }

    public Timestamp getEventTime() {
        return eventTime;
    }

    public void setEventTime(Timestamp eventTime) {
        this.eventTime = eventTime;
    }

    public Long getTimestamps() {
        return timestamps;
    }

    public void setTimestamps(Long timestamps) {
        this.timestamps = timestamps;
    }

    public Integer getTemperature() {
        return temperature;
    }

    public void setTemperature(Integer temperature) {
        this.temperature = temperature;
    }



    public Sensor2(String deviceId, Integer temperature, Long timestamps) {
        this.deviceId = deviceId;

        this.timestamps = timestamps;
    }

    public Sensor2() {
    }

    @Override
    public String toString() {
        return "Sensor2{" +
                "deviceId='" + deviceId + '\'' +
                ", temperature=" + temperature +
                ", timestamps=" + timestamps +
                ", eventTime=" + eventTime +
                '}';
    }
}
