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
    private Long timestamp;


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

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }


    public Sensor(String deviceId, Integer temperature, Long timestamp) {
        this.deviceId = deviceId;
        this.temperature = temperature;
        this.timestamp = timestamp;
    }

    public Sensor() {
    }

    @Override
    public String toString() {
        return "Sensor{" +
                "deviceId='" + deviceId + '\'' +
                ", temperature=" + temperature +
                ", timestamp=" + timestamp +
                '}';
    }
}
