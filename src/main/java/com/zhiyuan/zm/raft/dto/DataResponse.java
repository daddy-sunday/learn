package com.zhiyuan.zm.raft.dto;

import java.io.Serializable;

import com.zhiyuan.zm.raft.constant.StatusCode;

/**
 * 数据响应类（修正拼写）
 * @author zhouzhiyuan
 * @date 2021/11/23
 */
public class DataResponse implements Serializable {

  private int status;
  private String message;

  public DataResponse() {
  }

  public DataResponse(int status) {
    this.status = status;
  }

  public DataResponse(String message) {
    this.status = StatusCode.SUCCESS;
    this.message = message;
  }

  public DataResponse(int status, String message) {
    this.status = status;
    this.message = message;
  }

  /**
   * 判断响应是否成功
   * @return 成功返回 true，否则返回 false
   */
  public boolean isSuccess() {
    return this.status == StatusCode.SUCCESS;
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
    return "DataResponse{" +
        "status=" + status +
        ", message='" + message + '\'' +
        '}';
  }
}
