package org.example.raft.dto;

/**
 *@author zhouzhiyuan
 *@date 2021/10/22
 */
public class LogEntries {
  private long logIndex;

  private long term;

  private String messsage;

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


  public LogEntries() {
  }

  public LogEntries(long logIndex, long term, String messsage) {
    this.logIndex = logIndex;
    this.term = term;
    this.messsage = messsage;
  }

  public String getMesssage() {
    return messsage;
  }

  public void setMesssage(String messsage) {
    this.messsage = messsage;
  }
}
