package org.example.conf;

/**
 *@author zhouzhiyuan
 *@date 2021/10/22
 */
public class GlobalConfig {

 private String logPath = "C:\\Users\\zhouz\\Desktop\\raft\\log";

 private String dataPath = "C:\\Users\\zhouz\\Desktop\\raft\\data";

  /**
   * follow检查心跳间隔
   */
 private long checkTimeoutInterval = 600000;

 private String otherNode = "localhost:20000,localhost:20001,localhost:20002";

 private String currentNode = "localhost:20000";

 private int port = 20000;

  /**
   * 发送心跳间隔
   */
 private int sendHeartbeatInterval = 100000;

  /**
   * 保存log日志间隔
   */
 private long savelogTaskInterval = 3000;

  /**
   *同步log日志时间间隔
   */
 private long synLogTaskInterval = 3000;

  /**
   *应用log日志时间间隔
   */
 private long applyLogTaskInterval = 10000;

  /**
   *检查追日志的任务时间间隔
   */
 private long chaseAfterLogTaskInterval = 10000;

  /**
   * 发送日志超时时间
   */
 private int  sendHeartbeatTimeout = 10000;

  /**
   * 无服务等待时间间隔
   */
 private int  noServiceWaitTime = 1000;

  /**
   * 无服务等待超时时间
   */
 private int  noServiceTimeout = 10000;



  public int getSendHeartbeatTimeout() {
    return sendHeartbeatTimeout;
  }

  public void setSendHeartbeatTimeout(int sendHeartbeatTimeout) {
    this.sendHeartbeatTimeout = sendHeartbeatTimeout;
  }

  public void setNoServiceWaitTime(int noServiceWaitTime) {
    this.noServiceWaitTime = noServiceWaitTime;
  }

  public int getNoServiceTimeout() {
    return noServiceTimeout;
  }

  public void setNoServiceTimeout(int noServiceTimeout) {
    this.noServiceTimeout = noServiceTimeout;
  }

  public long getNoServiceWaitTime() {
    return noServiceWaitTime;
  }



  public long getSavelogTaskInterval() {
    return savelogTaskInterval;
  }

  public void setSavelogTaskInterval(long savelogTaskInterval) {
    this.savelogTaskInterval = savelogTaskInterval;
  }

  public long getSynLogTaskInterval() {
    return synLogTaskInterval;
  }

  public void setSynLogTaskInterval(long synLogTaskInterval) {
    this.synLogTaskInterval = synLogTaskInterval;
  }

  public long getApplyLogTaskInterval() {
    return applyLogTaskInterval;
  }

  public void setApplyLogTaskInterval(long applyLogTaskInterval) {
    this.applyLogTaskInterval = applyLogTaskInterval;
  }

  public long getChaseAfterLogTaskInterval() {
    return chaseAfterLogTaskInterval;
  }

  public void setChaseAfterLogTaskInterval(long chaseAfterLogTaskInterval) {
    this.chaseAfterLogTaskInterval = chaseAfterLogTaskInterval;
  }

  public int getSendHeartbeatInterval() {
    return sendHeartbeatInterval;
  }

  public void setSendHeartbeatInterval(int sendHeartbeatInterval) {
    this.sendHeartbeatInterval = sendHeartbeatInterval;
  }

  public String getOtherNode() {
    return otherNode;
  }

  public void setOtherNode(String otherNode) {
    this.otherNode = otherNode;
  }

  public long getCheckTimeoutInterval() {
    return checkTimeoutInterval;
  }

  public void setCheckTimeoutInterval(long checkTimeoutInterval) {
    this.checkTimeoutInterval = checkTimeoutInterval;
  }

  public String getLogPath() {
    return logPath;
  }

  public void setLogPath(String logPath) {
    this.logPath = logPath;
  }

  public String getDataPath() {
    return dataPath;
  }

  public void setDataPath(String dataPath) {
    this.dataPath = dataPath;
  }

  public String getCurrentNode() {
    return currentNode;
  }

  public void setCurrentNode(String currentNode) {
    this.currentNode = currentNode;
  }

  public int getPort() {
    return port;
  }

  public void setPort(int port) {
    this.port = port;
  }

  @Override
  public String
  toString() {
    return "GlobalConfig{" +
        "logPath='" + logPath + '\'' +
        ", dataPath='" + dataPath + '\'' +
        ", checkTimeoutInterval=" + checkTimeoutInterval +
        ", otherNode='" + otherNode + '\'' +
        ", currentNode='" + currentNode + '\'' +
        ", sendHeartbeatInterval=" + sendHeartbeatInterval +
        '}';
  }
}
