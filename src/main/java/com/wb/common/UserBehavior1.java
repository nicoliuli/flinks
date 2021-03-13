package com.wb.common;

public class UserBehavior1 {
    private Long user_id;
    private Long item_id;
    private Long category_id;
    private String behavior;
    /**
     * 事件时间
     */
    private Long ts;


    public Long getUser_id() {
        return user_id;
    }

    public void setUser_id(Long user_id) {
        this.user_id = user_id;
    }

    public Long getItem_id() {
        return item_id;
    }

    public void setItem_id(Long item_id) {
        this.item_id = item_id;
    }

    public Long getCategory_id() {
        return category_id;
    }

    public void setCategory_id(Long category_id) {
        this.category_id = category_id;
    }

    public String getBehavior() {
        return behavior;
    }

    public void setBehavior(String behavior) {
        this.behavior = behavior;
    }



    public UserBehavior1() {
    }

    public Long getTs() {
        return ts;
    }

    public void setTs(Long ts) {
        this.ts = ts;
    }

    public UserBehavior1(Long user_id, Long item_id, Long category_id, String behavior, Long ts) {
        this.user_id = user_id;
        this.item_id = item_id;
        this.category_id = category_id;
        this.behavior = behavior;
        this.ts = ts;
    }


}
