package org.example.raft.dto;

/**
 *@author zhouzhiyuan
 *@date 2021/11/26
 */
public class ChaseAfterLog {
  private String  address;
  private long logId;

  public ChaseAfterLog(String address, long logId) {
    this.address = address;
    this.logId = logId;
  }

  public String getAddress() {
    return address;
  }

  public void setAddress(String address) {
    this.address = address;
  }

  public long getLogId() {
    return logId;
  }

  public void setLogId(long logId) {
    this.logId = logId;
  }
}
