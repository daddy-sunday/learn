package org.example.raft.dto;

/**
 *@author zhouzhiyuan
 *@date 2021/11/23
 */
public class SetData {
  private int cmd;
  private Row[] rows;

  public SetData(int cmd, Row[] rows) {
    this.cmd = cmd;
    this.rows = rows;
  }
}
