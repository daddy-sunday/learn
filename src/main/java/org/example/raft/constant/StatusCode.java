package org.example.raft.constant;

/**
 *@author zhouzhiyuan
 *@date 2021/11/24
 */
public class StatusCode {
  public static final int SUCCESS = 200;

  public static final int REDIRECT = 301;

  public static final int SYSTEMEXCEPTION = 500;

  /**
   * 当集群处于选举状态时的返回码
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
