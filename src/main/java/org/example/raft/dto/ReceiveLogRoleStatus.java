package org.example.raft.dto;

/**
 *@author zhouzhiyuan
 *@date 2021/11/25
 */
public class ReceiveLogRoleStatus {
  private volatile long nextIndex;
  private volatile boolean isSyn;
}
