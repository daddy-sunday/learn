package org.example.raft.dto;

/**
 *@author zhouzhiyuan
 *@date 2021/10/22
 */
public class LogEntry {

  private int  cmd;

  private Row[] rows;

  public int getCmd() {
    return cmd;
  }

  public void setCmd(int cmd) {
    this.cmd = cmd;
  }

  public LogEntry(int cmd, Row[] rows) {
    this.cmd = cmd;
    this.rows = rows;
  }

  public LogEntry() {
  }

  public Row[] getRows() {
    return rows;
  }

  public void setRows(Row[] rows) {
    this.rows = rows;
  }
}
