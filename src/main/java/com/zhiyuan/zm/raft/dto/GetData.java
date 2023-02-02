package com.zhiyuan.zm.raft.dto;

/**
 *@author zhouzhiyuan
 *@date 2021/11/23
 */
public class GetData {
  private String key;

  public GetData(String key) {
    this.key = key;
  }

  public String getKey() {
    return key;
  }

  public void setKey(String key) {
    this.key = key;
  }
}
