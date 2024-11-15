package com.zhiyuan.zm.raft.role.event;

/**
 * @author zhouzhiyuan
 * @date 2024/11/15 16:42
 */
public class Event {
  private byte eventType;
  private Object object;

  public Event(byte eventType, Object object) {
    this.eventType = eventType;
    this.object = object;
  }

  public byte getEventType() {
    return eventType;
  }

  public void setEventType(byte eventType) {
    this.eventType = eventType;
  }

  public Object getObject() {
    return object;
  }

  public void setObject(Object object) {
    this.object = object;
  }
}
