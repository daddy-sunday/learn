package org.example.raft.dto;

import java.io.Serializable;

/**
 *@author zhouzhiyuan
 *@date 2021/10/21
 */
public class RaftRpcResponest implements Serializable {
  private long term;
  private Boolean success;

  public RaftRpcResponest(long term, Boolean succcess) {
    this.term = term;
    this.success = succcess;
  }

  public long getTerm() {
    return term;
  }

  public void setTerm(long term) {
    this.term = term;
  }

  public Boolean getSuccess() {
    return success;
  }

  public void setSuccess(Boolean success) {
    this.success = success;
  }
}
