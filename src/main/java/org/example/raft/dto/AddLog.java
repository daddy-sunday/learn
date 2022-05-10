package org.example.raft.dto;

import com.alibaba.fastjson.annotation.JSONField;

/**
 *@author zhouzhiyuan
 *@date 2021/10/22
 */
public class AddLog {

  private long logIndex;

  private long term;

  private String leaderId;

  private long prevLogIndex;

  private long preLogTerm;

  private LogEntry[] entries;

  private long leaderCommit;

  @JSONField(serialize = false,deserialize=false)
  private boolean exit = false;

  /**
   *
   * @param term
   * @param leaderId
   * @param prevLogIndex
   * @param preLogTerm
   * @param entries
   * @param leaderCommit
   */
  public AddLog(long logIndex,long term, String leaderId, long prevLogIndex, long preLogTerm,
      LogEntry[] entries, long leaderCommit) {
    this.logIndex = logIndex;
    this.term = term;
    this.leaderId = leaderId;
    this.prevLogIndex = prevLogIndex;
    this.preLogTerm = preLogTerm;
    this.entries = entries;
    this.leaderCommit = leaderCommit;
  }

  public AddLog(boolean exit) {
    this.exit = exit;
  }


  public boolean isExit() {
    return exit;
  }

  public void setExit(boolean exit) {
    this.exit = exit;
  }

  public AddLog(long term, String leaderId) {
    this.term = term;
    this.leaderId = leaderId;
  }

  public AddLog() {
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

  public LogEntry[] getEntries() {
    return entries;
  }

  public void setEntries(LogEntry[] entries) {
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
