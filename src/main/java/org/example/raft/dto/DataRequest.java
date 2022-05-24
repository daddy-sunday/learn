package org.example.raft.dto;

import java.io.Serializable;

/**
 *@author zhouzhiyuan
 *@date 2021/11/23
 */
public class DataRequest implements Serializable {
  private int type;

  private String message;

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
