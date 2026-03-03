package com.zhiyuan.zm.raft.dto;

import java.io.Serializable;

/**
 *@author zhouzhiyuan
 *@date 2021/11/23
 */
public class DataRequest implements Serializable {
  private int type;

  private String clientId;

  private String message;

  public DataRequest() {
  }

  public DataRequest(int type, String message) {
    this.type = type;
    this.message = message;
  }

  public DataRequest(int type, String clientId, String message) {
    this.type = type;
    this.clientId = clientId;
    this.message = message;
  }

  public String getClientId() {
    return clientId;
  }

  public void setClientId(String clientId) {
    this.clientId = clientId;
  }

  public int getType() {
    return type;
  }

  public void setType(int type) {
    this.type = type;
  }

  public String getMessage() {
    return message;
  }

  public void setMessage(String message) {
    this.message = message;
  }
}
