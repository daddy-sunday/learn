package org.example.raft.dto;

/**
 *@author zhouzhiyuan
 *@date 2021/10/22
 */
public class VoteRequest {

  private long term;
  private String candidateId;
  private long lastLogIndex;
  private long lastLogTerm;

  public VoteRequest() {
  }

  public VoteRequest(long term, String candidateId, long lastLogIndex, long lastLogTerm) {
    this.term = term;
    this.candidateId = candidateId;
    this.lastLogIndex = lastLogIndex;
    this.lastLogTerm = lastLogTerm;
  }

  public long getTerm() {
    return term;
  }

  public void setTerm(long term) {
    this.term = term;
  }

  public String getCandidateId() {
    return candidateId;
  }

  public void setCandidateId(String candidateId) {
    this.candidateId = candidateId;
  }

  public long getLastLogIndex() {
    return lastLogIndex;
  }

  public void setLastLogIndex(long lastLogIndex) {
    this.lastLogIndex = lastLogIndex;
  }

  public long getLastLogTerm() {
    return lastLogTerm;
  }

  public void setLastLogTerm(long lastLogTerm) {
    this.lastLogTerm = lastLogTerm;
  }

  @Override
  public String toString() {
    return "VoteRequest{" +
        "term=" + term +
        ", candidateId='" + candidateId + '\'' +
        ", lastLogIndex=" + lastLogIndex +
        ", lastLogTerm=" + lastLogTerm +
        '}';
  }
}
