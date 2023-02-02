package com.zhiyuan.zm.raft.service;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.zhiyuan.zm.raft.dto.ChaseAfterLog;

/**
 * @author zhouzhiyuan
 * @date 2021/11/19 //todo  优化考虑一下每个成员变量是否需要使用 volatile类型
 */
public class RaftStatus {

  /**
   * 0：离线，1：在线，
   */
  private volatile byte serviceStatus = 0;

  /**
   * 当前raft是否初始化
   */
  private boolean initFlag = false;

  /**
   * 为后面的多 raftgroup 做准备，先给个默认值
   */
  private int groupId = 1;


  private byte[] startKey;

  private byte[] endKey;

  /**
   * data persistence status
   */

  private volatile long currentTerm = 1;

  private String votedFor;


  private volatile long maxLogIndex;

  /**
   * easy lose status
   * todo  need init
   */
  private volatile long commitIndex = 0;

  /**
   * 最后应用的日志id
   */
  private volatile long appliedInedex = 1;


  /**
   * member
   */
  private List<String> allMembers = new LinkedList<>();

  private LinkedBlockingDeque<String> validMembers = new LinkedBlockingDeque<>();

  private LinkedBlockingDeque<ChaseAfterLog> failedMembers = new LinkedBlockingDeque<>();

  /**
   * 人员数量
   */
  private int personelNum;

  /**
   * 本机地址
   */
  private String localAddress;

  /**
   * leader地址，用于请求重定向
   */
  private volatile String leaderAddress;

  /**
   * follower 超时重新选举使用 ，每次同步心跳是更新这个时间
   */
  private volatile long lastUpdateTime;

  /**
   * follower 使用，当前存储的日中的最大的logindex
   */
  private volatile long lastTimeLogIndex;

  /**
   * follower 使用，当前存储的日中的最大的logindex的term
   */
  private volatile long lastTimeTerm;


  //debug 使用
  private ScheduledExecutorService scheduledExecutorService = new ScheduledThreadPoolExecutor(1,
      e -> new Thread(e, "debug-raftStatus"));


  public void initDebug() {
  scheduledExecutorService = new ScheduledThreadPoolExecutor(1,
        e -> new Thread(e, "debug-raftStatus"));
  scheduledExecutorService.scheduleAtFixedRate(this::run,2000,30000, TimeUnit.MILLISECONDS);
  }

  public void run(){
    toString();
  }


  public boolean isInitFlag() {
    return initFlag;
  }

  public void setInitFlag(boolean initFlag) {
    this.initFlag = initFlag;
  }

  public byte[] getStartKey() {
    return startKey;
  }

  public void setStartKey(byte[] startKey) {
    this.startKey = startKey;
  }

  public byte[] getEndKey() {
    return endKey;
  }

  public void setEndKey(byte[] endKey) {
    this.endKey = endKey;
  }

  public long getLastTimeLogIndex() {
    return lastTimeLogIndex;
  }

  public void setLastTimeLogIndex(long lastTimeLogIndex) {
    this.lastTimeLogIndex = lastTimeLogIndex;
  }

  public long getLastTimeTerm() {
    return lastTimeTerm;
  }

  public void setLastTimeTerm(long lastTimeTerm) {
    this.lastTimeTerm = lastTimeTerm;
  }

  public LinkedBlockingDeque<ChaseAfterLog> getFailedMembers() {
    return failedMembers;
  }

  public void setFailedMembers(LinkedBlockingDeque<ChaseAfterLog> failedMembers) {
    this.failedMembers = failedMembers;
  }

  public int getGroupId() {
    return groupId;
  }

  public void setGroupId(int groupId) {
    this.groupId = groupId;
  }

  public long getLastUpdateTime() {
    return lastUpdateTime;
  }

  public void setLastTime() {
    this.lastUpdateTime = System.currentTimeMillis();
  }

  public String getLeaderAddress() {
    return leaderAddress;
  }

  public void setLeaderAddress(String leaderAddress) {
    this.leaderAddress = leaderAddress;
  }

  public String getLocalAddress() {
    return localAddress;
  }

  public void setLocalAddress(String localAddress) {
    this.localAddress = localAddress;
  }

  public int getPersonelNum() {
    return personelNum;
  }

  public void setPersonelNum(int personelNum) {
    this.personelNum = personelNum;
  }

  public RaftStatus() {
  }

  public long getCurrentTerm() {
    return currentTerm;
  }

  public void setCurrentTerm(long currentTerm) {
    this.currentTerm = currentTerm;
  }

  public String getVotedFor() {
    return votedFor;
  }

  public void setVotedFor(String votedFor) {
    this.votedFor = votedFor;
  }

  public long getCommitIndex() {
    return commitIndex;
  }

  public void setCommitIndex(long commitIndex) {
    this.commitIndex = commitIndex;
  }

  public long getAppliedIndex() {
    return appliedInedex;
  }

  public void setAppliedIndex(long lastApplied) {
    this.appliedInedex = lastApplied;
  }

  public List<String> getAllMembers() {
    return allMembers;
  }

  public void setAllMembers(List<String> allMembers) {
    this.allMembers = allMembers;
  }

  public LinkedBlockingDeque<String> getValidMembers() {
    return validMembers;
  }

  public void setValidMembers(LinkedBlockingDeque<String> validMembers) {
    this.validMembers = validMembers;
  }

  public void currentTermAddOne() {
    currentTerm = currentTerm + 1;
  }

  public byte getServiceStatus() {
    return serviceStatus;
  }

  public void setServiceStatus(byte serviceStatus) {
    this.serviceStatus = serviceStatus;
  }

  public void setLastUpdateTime(long lastUpdateTime) {
    this.lastUpdateTime = lastUpdateTime;
  }

  @Override
  public String toString() {
    return "RaftStatus{" +
        "serviceStatus=" + serviceStatus +
        ", initFlag=" + initFlag +
        ", groupId=" + groupId +
        ", startKey=" + Arrays.toString(startKey) +
        ", endKey=" + Arrays.toString(endKey) +
        ", currentTerm=" + currentTerm +
        ", votedFor='" + votedFor + '\'' +
        ", maxLogIndex=" + maxLogIndex +
        ", commitIndex=" + commitIndex +
        ", appliedIndex=" + appliedInedex +
        ", allMembers=" + allMembers +
        ", validMembers=" + validMembers +
        ", failedMembers=" + failedMembers +
        ", personelNum=" + personelNum +
        ", localAddress='" + localAddress + '\'' +
        ", leaderAddress='" + leaderAddress + '\'' +
        ", lastUpdateTime=" + lastUpdateTime +
        ", lastTimeLogIndex=" + lastTimeLogIndex +
        ", lastTimeTerm=" + lastTimeTerm +
        '}';
  }
}
