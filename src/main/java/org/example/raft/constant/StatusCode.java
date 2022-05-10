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
  public static final int SLEEP = 600;

  /**
   * 同步log失败
   */
  public static final int SYNLOG = 601;

  public static final int SAVELOG = 602;

  public static final int EXCEPTION = 603;

}
