package com.wb.common;



import java.util.Map;

public class ChatMsg {
    private Integer count = 0;
    private String sid; // 消息序列号
    private int bizType; // 业务类型: 1:普通聊天; 2:通知;
    private int format; // 消息格式: 0:纯文本; 1:图片; 2:语音; 3:视频; 4:文件; 5:图文混排(H5); 6:骰子 7:消息提示 8:名片 9:游戏邀请 10+:widget
    private int chatType; // 聊天类型: 1:私聊; 2:群聊;
    private int msgType; // 消息类型: 0:聊天; 1:命令; 2:回执; 3:通知;
    private int offlineFlg; // 是否需要做离线处理: 0:不用离线; 1:如果不在线,做离线消息处理; 2:必须做离线消息处理
    private int pushFlg; // 是否需要push: 0:不需要; 1:离线的话需要; 2:强行push
    private int ackFlg; // 是否需要回执: 0:不需要; 1:需要收到消息应答; 2:需要送达应答
    private String from; //uid
    private String to; // 可能是uid,可能是chId
    private long time; //消息发送时间戳
    private int appId;//该字段在客户段没有用到
    private int appVer;//该字段在客户段没有用到
    private int bubble;//气泡ID
    private int offline;//消息是否是离线消息
    private Map<String, Object> body; // 正文

    public ChatMsg() {
    }

    public ChatMsg(String sid, int bizType, int format, int chatType, int msgType, int offlineFlg, int pushFlg, int ackFlg, String from, String to, long time, int appId, int appVer, int bubble, int offline, Map<String, Object> body) {
        this.sid = sid;
        this.bizType = bizType;
        this.format = format;
        this.chatType = chatType;
        this.msgType = msgType;
        this.offlineFlg = offlineFlg;
        this.pushFlg = pushFlg;
        this.ackFlg = ackFlg;
        this.from = from;
        this.to = to;
        this.time = time;
        this.appId = appId;
        this.appVer = appVer;
        this.bubble = bubble;
        this.offline = offline;
        this.body = body;
    }

    public String getSid() {
        return sid;
    }

    public void setSid(String sid) {
        this.sid = sid;
    }

    public int getBizType() {
        return bizType;
    }

    public void setBizType(int bizType) {
        this.bizType = bizType;
    }

    public int getFormat() {
        return format;
    }

    public void setFormat(int format) {
        this.format = format;
    }

    public int getChatType() {
        return chatType;
    }

    public void setChatType(int chatType) {
        this.chatType = chatType;
    }

    public int getMsgType() {
        return msgType;
    }

    public void setMsgType(int msgType) {
        this.msgType = msgType;
    }

    public int getOfflineFlg() {
        return offlineFlg;
    }

    public void setOfflineFlg(int offlineFlg) {
        this.offlineFlg = offlineFlg;
    }

    public int getPushFlg() {
        return pushFlg;
    }

    public void setPushFlg(int pushFlg) {
        this.pushFlg = pushFlg;
    }

    public int getAckFlg() {
        return ackFlg;
    }

    public void setAckFlg(int ackFlg) {
        this.ackFlg = ackFlg;
    }

    public String getFrom() {
        return from;
    }

    public void setFrom(String from) {
        this.from = from;
    }

    public String getTo() {
        return to;
    }

    public void setTo(String to) {
        this.to = to;
    }

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    public int getAppId() {
        return appId;
    }

    public void setAppId(int appId) {
        this.appId = appId;
    }

    public int getAppVer() {
        return appVer;
    }

    public void setAppVer(int appVer) {
        this.appVer = appVer;
    }

    public int getBubble() {
        return bubble;
    }

    public void setBubble(int bubble) {
        this.bubble = bubble;
    }

    public int getOffline() {
        return offline;
    }

    public void setOffline(int offline) {
        this.offline = offline;
    }

    public Map<String, Object> getBody() {
        return body;
    }

    public void setBody(Map<String, Object> body) {
        this.body = body;
    }

    public Integer getCount() {
        return count;
    }

    public void setCount(Integer count) {
        this.count = count;
    }
}
