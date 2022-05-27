package org.example.raft.dto;

import java.io.Serializable;

import org.example.raft.constant.StatusCode;

/**
 *@author zhouzhiyuan
 *@date 2021/11/23
 */
public class DataResponest implements Serializable {
  private int status;
  private String message;


  public DataResponest() {
  }

  public DataResponest(String message) {
    this.status = StatusCode.SUCCESS;
    this.message = message;
  }

  public DataResponest(int status, String message) {
    this.status = status;
    this.message = message;
  }

  public int getStatus() {
    return status;
  }

  public void setStatus(int status) {
    this.status = status;
  }

  public String getMessage() {
    return message;
  }

  public void setMessage(String message) {
    this.message = message;
  }

  @Override
  public String toString() {
    return "DataResponest{" +
        "status=" + status +
        ", message='" + message + '\'' +
        '}';
  }
}
