package org.example.raft.constant;

/**
 *@author zhouzhiyuan
 *@date 2022/5/12
 */
public class ServiceStatus {
  public static final byte IN_SERVICE = 1;
  public static final byte NON_SERVICE = 0;
  public static final byte IN_SWITCH_ROLE = 2;
  public static final byte WAIT_RENEW = 3;

}
