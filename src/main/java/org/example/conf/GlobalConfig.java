package org.example.conf;

/**
 *@author zhouzhiyuan
 *@date 2021/10/22
 */
public class GlobalConfig {

 private String logPath = "C:\\Users\\zhouz\\Desktop\\raft\\log";

 private String dataPath = "C:\\Users\\zhouz\\Desktop\\raft\\data";

 private long checkTimeoutInterval = 6000;

 private String otherNode = "localhost:20000,localhost:20001,localhost:20002";

 private String currentNode = "localhost:20000";

 private int port = 20000;

 private int sendHeartbeatInterval = 3000;

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
