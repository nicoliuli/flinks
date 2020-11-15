package com.wb.common;

public class Sensor {
    /**
     * 设备编号
     */
    private String deviceId;
    /**
     * 温度
     */
    private Integer temperature;

    /**
     * 时间戳
     */
    private Long timestamps;


    public String getDeviceId() {
        return deviceId;
    }

    public void setDeviceId(String deviceId) {
        this.deviceId = deviceId;
    }

    public Integer getTemperature() {
        return temperature;
    }

    public void setTemperature(Integer temperature) {
        this.temperature = temperature;
    }

    public Long getTimestamps() {
        return timestamps;
    }

    public void setTimestamps(Long timestamps) {
        this.timestamps = timestamps;
    }


    public Sensor(String deviceId, Integer temperature, Long timestamps) {
        this.deviceId = deviceId;
        this.temperature = temperature;
        this.timestamps = timestamps;
    }

    public Sensor() {
    }

    @Override
    public String toString() {
        return "Sensor{" +
                "deviceId='" + deviceId + '\'' +
                ", temperature=" + temperature +
                ", timestamp=" + timestamps +
                '}';
    }
}
