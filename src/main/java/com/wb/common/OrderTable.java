package com.wb.common;

public class OrderTable {

    /**
     * 订单id
     */
    private long id;
    /**
     * 商品id
     */
    private long shopid;
    /**
     * 下单时间
     */
    private long ts;


    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public long getShopid() {
        return shopid;
    }

    public void setShopid(long shopid) {
        this.shopid = shopid;
    }

    public long getTs() {
        return ts;
    }

    public void setTs(long ts) {
        this.ts = ts;
    }

    public OrderTable() {
    }

    public OrderTable(long id, long shopid, long ts) {
        this.id = id;
        this.shopid = shopid;
        this.ts = ts;
    }
}
