package org.example.raft.constant;

/**
 *@author zhouzhiyuan
 *@date 2021/10/28
 */
public class MessageType {
  public static final int VOTE = 1;
  public static final int LOG = 2;
  public static final int GET = 3;
  public static final int SET = 4;
  public static final int DELETE = 5;
  public static final int READ_INDEX = 6;
}
