package com.wb.common;

public class Device {
    private String deviceId;
    private Long nums;

    public Device() {
    }

    public Device(String deviceId, Long nums) {
        this.deviceId = deviceId;
        this.nums = nums;
    }

    public String getDeviceId() {
        return deviceId;
    }

    public void setDeviceId(String deviceId) {
        this.deviceId = deviceId;
    }

    public Long getNums() {
        return nums;
    }

    public void setNums(Long nums) {
        this.nums = nums;
    }
}
