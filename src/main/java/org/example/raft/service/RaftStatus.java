package org.example.raft.service;

import java.util.LinkedList;
import java.util.List;

import org.example.raft.dto.ChaseAfterLog;

/**
 *@author zhouzhiyuan
 *@date 2021/11/19
 * //todo  优化考虑一下每个成员变量是否需要使用 volatile类型
 */
public class RaftStatus {

  /**
   * 目前只有leader 有到了这个状态
   * 0：离线，1：在线，
   */
  private volatile byte serviceStatus = 0;

  /**
   * 为后面的多 raftgroup 做准备，先给个默认值
   */
  private int groupId = 1;

  /**
   *  data persistence status
   */

  private volatile long currentTerm = 1;

  private String votedFor;

  /**
   * easy lose status
   * todo  need init
   */
  private volatile long commitIndex = 1;

  /**
   * 最后应用的日志id
   */
  private volatile long lastApplied = 1;


  /**
   * member
   */
  private List<String> allMembers = new LinkedList<>();
  private volatile List<String> validMembers = new LinkedList<>();
  private List<ChaseAfterLog> failedMembers = new LinkedList<>();

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
  private volatile String  leaderAddress;

  /**
   *follower 超时重新选举使用 ，每次同步心跳是更新这个时间
   */
  private volatile long lastUpdateTime;

  /**
   *follower 使用，当前存储的日中的最大的logindex
   */
  private volatile long lastTimeLogIndex;

  /**
   *follower 使用，当前存储的日中的最大的logindex的term
   */
  private volatile long lastTimeTerm;


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

  public List<ChaseAfterLog> getFailedMembers() {
    return failedMembers;
  }

  public void setFailedMembers(List<ChaseAfterLog> failedMembers) {
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

  public long getLastApplied() {
    return lastApplied;
  }

  public void setLastApplied(long lastApplied) {
    this.lastApplied = lastApplied;
  }

  public List<String> getAllMembers() {
    return allMembers;
  }

  public void setAllMembers(List<String> allMembers) {
    this.allMembers = allMembers;
  }

  public List<String> getValidMembers() {
    return validMembers;
  }

  public void setValidMembers(List<String> validMembers) {
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
}
