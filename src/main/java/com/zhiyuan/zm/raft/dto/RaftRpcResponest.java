package com.zhiyuan.zm.raft.dto;

import java.io.Serializable;

/**
 *@author zhouzhiyuan
 *@date 2021/10/21
 */
public class RaftRpcResponest implements Serializable {
  private long term;
  private boolean status;
  private byte failCause;

  public RaftRpcResponest(long term) {
    this.term = term;
  }

  public RaftRpcResponest(long term, boolean succcess) {
    this.term = term;
    this.status = succcess;
  }

  public RaftRpcResponest(long term, boolean status, byte failCause) {
    this.term = term;
    this.status = status;
    this.failCause = failCause;
  }

  public long getTerm() {
    return term;
  }

  public void setTerm(long term) {
    this.term = term;
  }

  public boolean getStatus() {
    return status;
  }

  public void setStatus(boolean status) {
    this.status = status;
  }

  public boolean isStatus() {
    return status;
  }

  public byte getFailCause() {
    return failCause;
  }

  public void setFailCause(byte failCause) {
    this.failCause = failCause;
  }

  @Override
  public String toString() {
    return "RaftRpcResponest{" +
        "term=" + term +
        ", status=" + status +
        ", failCause=" + failCause +
        '}';
  }
}
