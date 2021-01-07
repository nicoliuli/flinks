package com.wb.common;

public class UserCnt {
    private String uid;
    private Long cnt;

    public UserCnt() {
    }

    public UserCnt(String uid, Long cnt) {
        this.uid = uid;
        this.cnt = cnt;
    }

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    public Long getCnt() {
        return cnt;
    }

    public void setCnt(Long cnt) {
        this.cnt = cnt;
    }
}
