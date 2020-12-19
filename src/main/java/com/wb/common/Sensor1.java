package com.wb.common;

public class Sensor1 {
    /**
     * 设备编号
     */
    private String deviceId;


    private Integer tp;

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



    public Long getTimestamps() {
        return timestamps;
    }

    public void setTimestamps(Long timestamps) {
        this.timestamps = timestamps;
    }

    public Integer getTp() {
        return tp;
    }

    public void setTp(Integer tp) {
        this.tp = tp;
    }

    public Sensor1(String deviceId, Integer temperature, Long timestamps) {
        this.deviceId = deviceId;

        this.timestamps = timestamps;
    }

    public Sensor1() {
    }

    @Override
    public String toString() {
        return "Sensor1{" +
                "deviceId='" + deviceId + '\'' +
                ", tp=" + tp +
                ", timestamps=" + timestamps +
                '}';
    }
}
