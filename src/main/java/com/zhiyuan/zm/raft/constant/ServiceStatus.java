package com.zhiyuan.zm.raft.constant;

/**
 *@author zhouzhiyuan
 *@date 2022/5/12
 */
public class ServiceStatus {
  /**
   * 对外服务状态
   */
  public static final byte IN_SERVICE = 1;

  /**
   * 不对外提供服务
   */
  public static final byte NON_SERVICE = 0;


  /**
   * 选举状态
   */
  public static final byte IN_SWITCH_ROLE = 2;

  /**
   * 日志冲突等待重构应用数据
   */
  public static final byte WAIT_RENEW = 3;

  /**
   * 只读：可以读取数据，不可以写入数据
   */
  public static final byte READ_ONLY = 4;

}
