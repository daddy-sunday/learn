package com.zhiyuan.zm.raft.dto;

/**
 *@author zhouzhiyuan
 *@date 2022/5/12
 */
public class Command {

  private int  cmd;

  private Row[] rows;

  public Command() {
  }

  public Command(int cmd) {
    this.cmd = cmd;
  }

  public Command(int cmd, Row[] rows) {
    this.cmd = cmd;
    this.rows = rows;
  }

  public int getCmd() {
    return cmd;
  }

  public void setCmd(int cmd) {
    this.cmd = cmd;
  }

  public Row[] getRows() {
    return rows;
  }

  public void setRows(Row[] rows) {
    this.rows = rows;
  }
}
