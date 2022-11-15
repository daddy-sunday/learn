package org.example.conf;

/**
 *@author zhouzhiyuan
 *@date 2021/10/22
 */
public class GlobalConfig {

 private String logPath = "D:\\tmp\\raft\\log";

 private String dataPath = "D:\\tmp\\raft\\data";

  /**
   * follow检查心跳间隔
   */
 private long checkTimeoutInterval = 20000;

 private String otherNode = "localhost:20000,localhost:20001,localhost:20002";

 private String currentNode = "localhost:20000";

 private int port = 20000;

  /**
   * 发送心跳间隔
   */
 private int sendHeartbeatInterval = 10000;

  /**
   * 保存log日志间隔
   */
 private long savelogTaskInterval = 50;

  /**
   *同步log日志时间间隔
   */
 private long synLogTaskInterval = 50;

  /**
   *应用log日志时间间隔
   */
 private long applyLogTaskInterval = 50;

  /**
   *检查追日志的任务时间间隔
   */
 private long chaseAfterLogTaskInterval = 30000;

  /**
   * 发送日志超时时间
   */
 private int  sendHeartbeatTimeout = 10000;

  /**
   * 无服务等待检查时间间隔(毫秒)
   */
 private int waitTimeInterval = 100;

  /**
   * 无服务等待检查次数 ，noServiceWaitTime * noServiceTimeout = 无服务等待时间
   */
 private int waitCount = 100;




  public int getSendHeartbeatTimeout() {
    return sendHeartbeatTimeout;
  }

  public void setSendHeartbeatTimeout(int sendHeartbeatTimeout) {
    this.sendHeartbeatTimeout = sendHeartbeatTimeout;
  }

  public int getWaitTimeInterval() {
    return waitTimeInterval;
  }

  public void setWaitTimeInterval(int waitTimeInterval) {
    this.waitTimeInterval = waitTimeInterval;
  }

  public int getWaitCount() {
    return waitCount;
  }

  public void setWaitCount(int waitCount) {
    this.waitCount = waitCount;
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
