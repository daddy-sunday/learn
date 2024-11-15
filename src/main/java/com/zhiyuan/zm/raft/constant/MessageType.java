package com.zhiyuan.zm.raft.constant;

/**
 *@author zhouzhiyuan
 *@date 2021/10/28
 */
public class MessageType {
  public static final int VOTE = 1;
  public static final int LOG = 2;

  public static final int GET = 3;
  public static final int SET = 4;
  public static final int READ_INDEX = 6;
  public static final int LEADER_MOVE = 7;
  public static final int RAFT_INFO = 8;

  public static final int OPEN_TRANSACTION = 9;
  public static final int COMMIT_TRANSACTION = 10;
  public static final int ROLLBACK_TRANSACTION = 11;




}
