package com.zhiyuan.zm.raft.constant;

/**
 *@author zhouzhiyuan
 *@date 2021/11/24
 */
public class StatusCode {
  public static final int SUCCESS = 200;

  public static final int REDIRECT = 301;

  public static final int SYSTEMEXCEPTION = 500;

  /**
   * 建议客户端等一会儿重试
   */
  public static final int SLEEP = 60;

  /**
   * 同步log失败
   */
  public static final int SYNLOG = 61;

  public static final int SAVELOG = 62;

  public static final byte EMPTY = 0;

  public static final int UNSUPPORT_REQUEST_TYPE =  -1;

  public static final int UNSUPPORT_REQUEST_FUNCATION =  501;

  public static final int RAFT_UNABLE_SERVER =  600;

  public static final int NON_SEVICE =  601;

  public static final int ERROR_REQUEST = 602;



  public static final int NOT_LEADER = 606;

  /**
   * leader 漂移
   */
  public static final int LEADER_MOVE = 605;

  /**
   * 等待超时
   */
  public static final int WAIT_TIME_OUT = 603;

  /**
   * 接收端异常
   */
  public static final byte SERVICE_EXCEPTION= 62;
  /**
   * 网络原因异常
   */
  public static final byte EXCEPTION = 63;

  /**
   * term小于接收者term
   */
  public static final byte MIN_TERM = 64;

  /**
   *不匹配logindex
   */
  public static final byte NOT_MATCH_LOG_INDEX = 65;
}
