package com.zhiyuan.zm.raft.role;

/**
 *@author zhouzhiyuan
 *@date 2021/10/22
 */
public class RoleStatus {

  /**
   * 领导
   */
  public static final int LEADER = 1;

  /**
   * 跟随者
   */
  public static final int FOLLOWER = 2;

  /**
   * 选举用
   */
  public static final int CANDIDATE = 3;

  /**
   * 学习者只能被动的接受leader 同步的日志条目和控制命令，不参与选举，也不算大多数
   */
  public static final int LEARNER = 4;

  /**
   * node  status
   */
  private  volatile int defaultStatus = FOLLOWER;


  public  boolean leaderToFollower() {
    if (defaultStatus == LEADER) {
      synchronized (RoleStatus.class) {
        if (defaultStatus == LEADER) {
          defaultStatus = FOLLOWER;
          return true;
        }
      }
    }
    return false;
  }

  public  void followerToCandidate() {
    if (defaultStatus == FOLLOWER) {
      synchronized (RoleStatus.class) {
        if (defaultStatus == FOLLOWER) {
          defaultStatus = CANDIDATE;
        }
      }
    }
  }

  public  boolean followerToLeader() {
    if (defaultStatus == FOLLOWER) {
      synchronized (RoleStatus.class) {
        if (defaultStatus == FOLLOWER) {
          defaultStatus = LEADER;
          return true;
        }
      }
    }
    return false;
  }


  public  boolean candidateToFollower() {
    if (defaultStatus == CANDIDATE) {
      synchronized (RoleStatus.class) {
        if (defaultStatus == CANDIDATE) {
          defaultStatus = FOLLOWER;
          return true;
        }
      }
    }
    return false;
  }
  public  void candidateToLeader() {
    if (defaultStatus == CANDIDATE) {
      synchronized (RoleStatus.class) {
        if (defaultStatus == CANDIDATE) {
          defaultStatus = LEADER;
        }
      }
    }
  }

  public  int getNodeStatus() {
    return defaultStatus;
  }
}
