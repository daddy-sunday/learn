package org.example.raft.dto;

/**
 *@author zhouzhiyuan
 *@date 2021/11/26
 */
public class SynchronizeLogResult {
  private String address;
  private boolean success;
  private int  statusCode = -1;

  public SynchronizeLogResult() {
  }

  public SynchronizeLogResult(String address, boolean success) {
    this.address = address;
    this.success = success;
  }

  public SynchronizeLogResult(String address, boolean success, int statusCode) {
    this.address = address;
    this.success = success;
    this.statusCode = statusCode;
  }


  public int getStatusCode() {
    return statusCode;
  }

  public void setStatusCode(int statusCode) {
    this.statusCode = statusCode;
  }

  public String getAddress() {
    return address;
  }

  public void setAddress(String address) {
    this.address = address;
  }

  public boolean isSuccess() {
    return success;
  }

  public void setSuccess(boolean success) {
    this.success = success;
  }
}
