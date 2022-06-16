package org.example.raft.dto;

/**
 *@author zhouzhiyuan
 *@date 2022/6/15
 */
public class DataChangeDto {
  private long commitIndex;
  private long lastTimeLogIndex;
  private long lastTimeTerm;

  public DataChangeDto() {
  }

  public DataChangeDto(long commitIndex) {
    this.commitIndex = commitIndex;
  }

  public DataChangeDto(long lastTimeLogIndex, long lastTimeTerm) {
    this.lastTimeLogIndex = lastTimeLogIndex;
    this.lastTimeTerm = lastTimeTerm;
  }


  public long getCommitIndex() {
    return commitIndex;
  }

  public void setCommitIndex(long commitIndex) {
    this.commitIndex = commitIndex;
  }

  public long getLastTimeLogIndex() {
    return lastTimeLogIndex;
  }

  public void setLastTimeLogIndex(long lastTimeLogIndex) {
    this.lastTimeLogIndex = lastTimeLogIndex;
  }

  public long getLastTimeTerm() {
    return lastTimeTerm;
  }

  public void setLastTimeTerm(long lastTimeTerm) {
    this.lastTimeTerm = lastTimeTerm;
  }
}
