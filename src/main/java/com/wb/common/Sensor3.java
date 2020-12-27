package com.wb.common;

import java.sql.Timestamp;

public class Sensor3 {
    /**
     * 设备编号
     */
    private String deviceId;


    private Integer temperature;

    /**
     * 时间戳
     */
    private Timestamp timestamps;




    public String getDeviceId() {
        return deviceId;
    }

    public void setDeviceId(String deviceId) {
        this.deviceId = deviceId;
    }


    public Timestamp getTimestamps() {
        return timestamps;
    }

    public void setTimestamps(Timestamp timestamps) {
        this.timestamps = timestamps;
    }

    public Integer getTemperature() {
        return temperature;
    }

    public void setTemperature(Integer temperature) {
        this.temperature = temperature;
    }



    public Sensor3(String deviceId, Integer temperature, Timestamp timestamps) {
        this.deviceId = deviceId;

        this.timestamps = timestamps;
    }

    public Sensor3() {
    }

    @Override
    public String toString() {
        return "Sensor2{" +
                "deviceId='" + deviceId + '\'' +
                ", temperature=" + temperature +
                ", timestamps=" + timestamps +
                '}';
    }
}
