package com.dianping.squirrel.client.config.zookeeper;

public class CacheEvent {
    private Object content;

    private CacheEventType type;

    public CacheEvent() {
    }

    public CacheEvent(CacheEventType type, Object content) {
        this.type = type;
        this.content = content;
    }

    public CacheEventType getType() {
        return type;
    }

    public void setType(CacheEventType type) {
        this.type = type;
    }

    public Object getContent() {
        return content;
    }

    public void setContent(Object content) {
        this.content = content;
    }

    public enum CacheEventType {
        ServiceChange, ServiceRewriteChange, ServiceWeightChange,
        CategoryChange, FlowControlChange, VersionChange, KeyRemove,
        BatchKeyRemove, HotKeyChange, AppClientConfigChange, GroupConfigChange
    }
}
