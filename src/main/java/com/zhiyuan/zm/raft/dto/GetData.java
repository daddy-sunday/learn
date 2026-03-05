package com.zhiyuan.zm.raft.dto;

/**
 *@author zhouzhiyuan
 *@date 2021/11/23
 */
public class GetData {
  private String key;

  /**
   * 客户端标识（用于事务操作）
   */
  private String clientId;

  public GetData(String key) {
    this.key = key;
  }

  public GetData(String key, String clientId) {
    this.key = key;
    this.clientId = clientId;
  }

  public String getKey() {
    return key;
  }

  public void setKey(String key) {
    this.key = key;
  }

  public String getClientId() {
    return clientId;
  }

  public void setClientId(String clientId) {
    this.clientId = clientId;
  }
}
