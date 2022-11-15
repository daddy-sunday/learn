package org.example.raft.dto;

/**
 *@author zhouzhiyuan
 *@date 2021/11/26
 */
public class ChaseAfterLog {
  private String  address;
  private int raftGroupId;
  private long logId;

  public ChaseAfterLog(String address, int raftGroupId, long logId) {
    this.address = address;
    this.raftGroupId = raftGroupId;
    this.logId = logId;
  }

  public int getRaftGroupId() {
    return raftGroupId;
  }

  public void setRaftGroupId(int raftGroupId) {
    this.raftGroupId = raftGroupId;
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

  @Override
  public String toString() {
    return "ChaseAfterLog{" +
        "address='" + address + '\'' +
        ", raftGroupId=" + raftGroupId +
        ", logId=" + logId +
        '}';
  }
}
