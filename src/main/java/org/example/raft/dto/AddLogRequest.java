package org.example.raft.dto;

/**
 *@author zhouzhiyuan
 *@date 2021/10/22
 */
public class AddLogRequest {

  private long logIndex;

  private long term;

  private String leaderId;

  private long prevLogIndex;

  private long preLogTerm;

  private LogEntries[] entries;

  private long leaderCommit;

  /**
   *
   * @param term
   * @param leaderId
   * @param prevLogIndex
   * @param preLogTerm
   * @param entries
   * @param leaderCommit
   */
  public AddLogRequest(long logIndex,long term, String leaderId, long prevLogIndex, long preLogTerm,
      LogEntries[] entries, long leaderCommit) {
    this.logIndex = logIndex;
    this.term = term;
    this.leaderId = leaderId;
    this.prevLogIndex = prevLogIndex;
    this.preLogTerm = preLogTerm;
    this.entries = entries;
    this.leaderCommit = leaderCommit;
  }

  public AddLogRequest(long term, String leaderId) {
    this.term = term;
    this.leaderId = leaderId;
  }

  public AddLogRequest() {
  }

  public long getLogIndex() {
    return logIndex;
  }

  public void setLogIndex(long logIndex) {
    this.logIndex = logIndex;
  }

  public long getTerm() {
    return term;
  }

  public void setTerm(long term) {
    this.term = term;
  }

  public String getLeaderId() {
    return leaderId;
  }

  public void setLeaderId(String leaderId) {
    this.leaderId = leaderId;
  }

  public long getPrevLogIndex() {
    return prevLogIndex;
  }

  public void setPrevLogIndex(long prevLogIndex) {
    this.prevLogIndex = prevLogIndex;
  }

  public long getPreLogTerm() {
    return preLogTerm;
  }

  public void setPreLogTerm(long preLogTerm) {
    this.preLogTerm = preLogTerm;
  }

  public LogEntries[] getEntries() {
    return entries;
  }

  public void setEntries(LogEntries[] entries) {
    this.entries = entries;
  }

  public long getLeaderCommit() {
    return leaderCommit;
  }

  public void setLeaderCommit(long leaderCommit) {
    this.leaderCommit = leaderCommit;
  }

  @Override
  public String toString() {
    return "AddLog{" +
        "logIndex=" + logIndex +
        ", term=" + term +
        ", leaderId='" + leaderId + '\'' +
        ", prevLogIndex=" + prevLogIndex +
        ", preLogTerm=" + preLogTerm +
        ", entries=" + entries +
        ", leaderCommit=" + leaderCommit +
        '}';
  }
}
